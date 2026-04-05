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
};


////////////////////////////////////////////////////////////////////////////////////////
#endif /* ccodecstream_h */
