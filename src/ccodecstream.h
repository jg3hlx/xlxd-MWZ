//
//  ccodecstream.h
//  xlxd
//
//  Created by Jean-Luc Deltombe (LX3JL) on 13/04/2017.
//  Copyright © 2015 Jean-Luc Deltombe (LX3JL). All rights reserved.
//
// ----------------------------------------------------------------------------
//    This file is part of xlxd.
//
//    xlxd is free software: you can redistribute it and/or modify
//    it under the terms of the GNU General Public License as published by
//    the Free Software Foundation, either version 3 of the License, or
//    (at your option) any later version.
//
//    xlxd is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//    GNU General Public License for more details.
//
//    You should have received a copy of the GNU General Public License
//    along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
// ----------------------------------------------------------------------------

#ifndef ccodecstream_h
#define ccodecstream_h

#include <atomic>
#include <queue>
#include <chrono>
#include "csemaphore.h"
#include "cudpsocket.h"
#include "cpacketqueue.h"
#include "cdstarslowdata.h"

////////////////////////////////////////////////////////////////////////////////////////
// forward declarations

class CDvFramePacket;  // pointer-only use for m_pLastVoiceInJitter

////////////////////////////////////////////////////////////////////////////////////////
// define

// Jitter buffer configuration
// JITTER_BUFFER_DELAY_MS: Initial buffering delay before releasing packets.
// This absorbs ambed latency variation (observed 8-144ms range).
// Set to ~2x average ambed latency to handle most jitter.
#define JITTER_BUFFER_DELAY_MS      160

// JITTER_BUFFER_FRAME_MS: Interval between releasing packets (voice frame duration)
#define JITTER_BUFFER_FRAME_MS      20

// frame sizes
#define AMBE_SIZE           9
#define AMBEPLUS_SIZE       9
#define IMBE_SIZE           11

// transcoder packet offsets
#define TRANSCODER_PACKET_CODEC_OFFSET      0
#define TRANSCODER_PACKET_PID_OFFSET        1
#define TRANSCODER_PACKET_AMBE1_OFFSET      2
#define TRANSCODER_PACKET_CODEC2_OFFSET     11
#define TRANSCODER_PACKET_AMBE2_OFFSET      12
#define TRANSCODER_PACKET_CODEC3_OFFSET     20
#define TRANSCODER_PACKET_AMBE3_OFFSET      21

// transcoder packet sizes
#define TRANSCODER_PACKET_SIZE_LEGACY       11      // codec(1) + pid(1) + ambe(9)
#define TRANSCODER_PACKET_SIZE_MULTI        20      // codec1(1) + pid(1) + ambe1(9) + codec2(1) + codec2_data(8)
#define TRANSCODER_PACKET_SIZE_C2IN         21      // codec1(1) + pid(1) + ambe1(9) + codec2(1) + ambe2(9)
#define TRANSCODER_PACKET_SIZE_IMBEIN       30      // codec1(1) + pid(1) + ambe1(9) + codec2(1) + ambe2(9) + codec3(1) + codec2_data(8)
#define TRANSCODER_PACKET_SIZE_3CODEC       32      // codec1(1) + pid(1) + ambe1(9) + codec2(1) + codec2_data(8) + codec3(1) + imbe(11)
#define TRANSCODER_PACKET_SIZE_C2IN_3CODEC  33      // codec1(1) + pid(1) + ambe1(9) + codec2(1) + ambe2(9) + codec3(1) + imbe(11)


////////////////////////////////////////////////////////////////////////////////////////
// class

class CPacketStream;

class CCodecStream : public CPacketQueue
{
public:
    // constructor
    CCodecStream(CPacketStream *, uint16, uint8, uint8, uint8, uint8);

    // destructor
    virtual ~CCodecStream();
    
    // initialization
    bool Init(uint16);
    void Close(void);
    
    // get
    bool   IsConnected(void) const          { return m_bConnected; }
    uint16 GetStreamId(void) const          { return m_uiStreamId; }
    double GetPingMin(void) const           { return m_fPingMin; }
    double GetPingMax(void) const           { return m_fPingMax; }
    double GetPingAve(void) const           { return (m_fPingCount != 0) ? m_fPingSum/m_fPingCount : 0; }
    uint32 GetTotalPackets(void) const      { return m_uiTotalPackets; }
    uint32 GetTimeoutPackets(void) const    { return m_uiTimeoutPackets; }
    uint32 GetReturnedPackets(void) const   { return m_uiReturnedPackets; }
    bool   IsEmpty(void) const;
    size_t GetDepth(void) const;

    // task
    static void Thread(CCodecStream *);
    void Task(void);
    

protected:
    // packet decoding helpers
    bool IsValidAmbePacket(const CBuffer &, uint8 *, uint8 *, uint8 *);

    // packet encoding helpers
    void EncodeAmbePacket(CBuffer *, const uint8 *);


protected:
    // data
    uint16          m_uiStreamId;
    uint16          m_uiPort;
    uint8           m_uiPid;
    uint8           m_uiCodecIn;
    uint8           m_uiCodecOut1;
    uint8           m_uiCodecOut2;
    uint8           m_uiCodecOut3;

    // socket
    CIp             m_Ip;
    CUdpSocket      m_Socket;
    std::atomic<bool> m_bConnected;
    
    // associated packet stream
    CPacketStream   *m_PacketStream;
    CPacketQueue    m_LocalQueue;

    // thread
    std::atomic<bool> m_bStopThread;
    std::thread     *m_pThread;
    CTimePoint      m_TimeoutTimer;
    CTimePoint      m_StatsTimer;
    
    // statistics
    double          m_fPingMin;
    double          m_fPingMax;
    double          m_fPingSum;
    double          m_fPingCount;
    uint32          m_uiTotalPackets;
    uint32          m_uiTimeoutPackets;
    uint32          m_uiReturnedPackets;

    // Jitter buffer - smooths out variable ambed latency
    // Packets are queued here with target release times, then released
    // at regular 20ms intervals to ensure smooth playback
    std::queue<CPacket *>  m_JitterBuffer;
    std::chrono::steady_clock::time_point m_NextReleaseTime;
    bool            m_bJitterBufferStarted;

    // In-flight counter: packets popped from jitter buffer but not yet
    // pushed to the packet stream. Prevents IsEmpty() from returning true
    // during the Phase 2 gap between jitter-pop and stream-push.
    std::atomic<int> m_iInFlightPackets;

    // D-Star slow-data synthesiser. Only used on AMBE+ egress (i.e. when
    // transcoding from a non-D-Star source to D-Star). For every AMBE+
    // output frame, the Task() loop stamps either the sync marker (frame
    // 0 of the 21-frame cycle) or 3 bytes of scrambled slow-data (frames
    // 1–20) into the outgoing DvData field. Initialised lazily on the
    // first transcoded frame via InitSlowData().
    CDStarSlowData  m_SlowData;
    bool            m_bSlowDataInit;

    // Cycle index for slow-data scheduling. Cycle 0 carries the text
    // message (BeginTextCycle); cycles 1+ carry header-sync continuously
    // (BeginHeaderCycle), matching the native D-Star radio pattern
    // confirmed by multi-cycle pcap of a Pi-Star source. Incremented on
    // each sync-frame boundary in Task(); reset to 0 by InitSlowData()
    // at the start of each stream.
    uint32          m_uiSlowDataCycle;

    // Pre-EoT slow-data marker support (all egress paths).
    //
    // Native D-Star radios emit a specific slow-data pattern in the
    // slow-data slot of the last voice frame immediately before the EoT
    // frame: wire bytes 0x55 0x55 0x55 (plaintext 0x25 0x1A 0xC6, the
    // mirror of the EoT frame's own slow-data pattern). Strict Icom
    // decoders (RP2C via g2_link) appear to use this as the "stream is
    // about to close cleanly" signal; without it, subsequent TXs can be
    // rejected/muted until the RP2C's activity timeout fires.
    //
    // Tracking and override now run for ALL streams that pass through
    // CCodecStream — both cross-mode AMBE+ output (where slow-data is
    // synthesised) and AMBE+ pass-through paths (native D-Star, DCS
    // source, XLX-interlinked AMBE+ traffic such as BM-to-XLX DMR
    // bridges that don't emit the marker themselves). For non-D-Star
    // egress the DvData mutation is harmless because those encoders
    // don't read DvData. See ccodecstream.cpp:512-578 for the full
    // rationale.
    //
    // We don't know which voice frame is the last one until the EoT
    // arrives. So we track a non-owning pointer to the most recently
    // pushed voice frame while it is still sitting in m_JitterBuffer
    // (it sits there for at least JITTER_BUFFER_DELAY_MS = 160ms before
    // first release). When an EoT arrives in Task() Phase 1, if the
    // pointer is non-NULL we overwrite that parked packet's slow-data
    // in-place with the pre-EoT marker before releasing it. The pointer
    // is cleared when the packet is popped from m_JitterBuffer (Phase 2)
    // or when the EoT override has already consumed it.
    //
    // Access is serialised by m_Mutex (CCodecStream::Lock()): all three
    // touch points (Phase 1 push-to-jitter, Phase 1 EoT override,
    // Phase 2 pop-from-jitter) hold the lock for the entire operation.
    // The pointer is strictly a tracking aid — it never owns the packet
    // and is always cleared before the packet is destroyed.
    CDvFramePacket *m_pLastVoiceInJitter;

    // Build the 20-char text + 9-element header-sync from the packet
    // stream's cached header and g_Reflector state, push them into
    // m_SlowData via SetText() and SetHeaderSync(). Safe to call
    // repeatedly — re-seeds both buffers each time.
    void InitSlowData(void);
};


////////////////////////////////////////////////////////////////////////////////////////
#endif /* ccodecstream_h */
