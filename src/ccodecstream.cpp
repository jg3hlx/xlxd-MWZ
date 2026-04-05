//
//  ccodecstream.cpp
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

#include "main.h"
#include <string.h>
#include <vector>
#include "ccodecstream.h"
#include "cdvframepacket.h"
#include "creflector.h"

////////////////////////////////////////////////////////////////////////////////////////
// define



////////////////////////////////////////////////////////////////////////////////////////
// constructor

CCodecStream::CCodecStream(CPacketStream *PacketStream, uint16 uiId, uint8 uiCodecIn, uint8 uiCodecOut1, uint8 uiCodecOut2, uint8 uiCodecOut3)
{
    m_bStopThread = false;
    m_pThread = NULL;
    m_uiStreamId = uiId;
    m_uiPid = 0;
    m_uiCodecIn = uiCodecIn;
    m_uiCodecOut1 = uiCodecOut1;
    m_uiCodecOut2 = uiCodecOut2;
    m_uiCodecOut3 = uiCodecOut3;
    m_bConnected = false;
    m_fPingMin = -1;
    m_fPingMax = -1;
    m_fPingSum = 0;
    m_fPingCount = 0;
    m_uiTotalPackets = 0;
    m_uiTimeoutPackets = 0;
    m_uiReturnedPackets = 0;
    m_PacketStream = PacketStream;
    m_bJitterBufferStarted = false;
    m_iInFlightPackets = 0;
}

////////////////////////////////////////////////////////////////////////////////////////
// destructor

CCodecStream::~CCodecStream()
{
    // stop thread first so it doesn't use the socket or queues
    m_bStopThread = true;
    if ( m_pThread != NULL )
    {
        m_pThread->join();
        delete m_pThread;
        m_pThread = NULL;
    }

    // close socket only after thread is gone
    m_Socket.Close();
    
    // empty local queue - these are packets sent to ambed but not yet returned
    int localQueueLost = 0;
    while ( !m_LocalQueue.empty() )
    {
        delete m_LocalQueue.front();
        m_LocalQueue.pop();
        localQueueLost++;
    }
    // empty ourselves - these are packets waiting to be sent to ambed
    int mainQueueLost = 0;
    while ( !empty() )
    {
        delete front();
        pop();
        mainQueueLost++;
    }
    // empty jitter buffer - these are transcoded packets waiting to be released
    int jitterQueueLost = 0;
    while ( !m_JitterBuffer.empty() )
    {
        delete m_JitterBuffer.front();
        m_JitterBuffer.pop();
        jitterQueueLost++;
    }
    // Log if any packets were lost at stream close
    int totalLost = localQueueLost + mainQueueLost + jitterQueueLost;
    if ( totalLost > 0 )
    {
        std::cout << "ambed WARNING: " << totalLost << " packet" << (totalLost > 1 ? "s" : "")
                  << " lost at stream close ("
                  << localQueueLost << " awaiting ambed response, "
                  << mainQueueLost << " unsent, "
                  << jitterQueueLost << " in jitter buffer)" << std::endl;
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// initialization

bool CCodecStream::Init(uint16 uiPort)
{
    bool ok;
    
    // reset stop flag
    m_bStopThread = false;
    
    // create server's IP
    m_Ip = g_Reflector.GetTranscoderIp();
    m_uiPort = uiPort;
    
    // create our socket
    ok = m_Socket.Open(uiPort);
    if ( ok )
    {
        // flush any stale packets from previous stream on this port
        CBuffer flushBuffer;
        CIp flushIp;
        while ( m_Socket.Receive(&flushBuffer, &flushIp, 0) != -1 )
        {
            // discard stale packets
        }

        // init timers
        m_TimeoutTimer.Now();

        // start thread and wait for it to be ready before accepting packets
        m_pThread = new std::thread(CCodecStream::Thread, this);
        int waitCount = 0;
        while ( !m_bConnected && waitCount < 1000 )
        {
            std::this_thread::yield();
            waitCount++;
        }
        if ( !m_bConnected )
        {
            std::cout << "Error: codec thread failed to start on port UDP" << uiPort << std::endl;
            m_bStopThread = true;
            m_pThread->join();
            delete m_pThread;
            m_pThread = NULL;
            m_Socket.Close();
            ok = false;
        }
    }
    else
    {
        std::cout << "Error opening socket on port UDP" << uiPort << " on ip " << g_Reflector.GetListenIp() << std::endl;
        m_bConnected = false;
    }
    
    // done
    return ok;
}

void CCodecStream::Close(void)
{
    // close socket
    m_bConnected = false;
    m_Socket.Close();
    
    // kill threads
    m_bStopThread = true;
    if ( m_pThread != NULL )
    {
        m_pThread->join();
        delete m_pThread;
        m_pThread = NULL;
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// get

bool CCodecStream::IsEmpty(void) const
{
    // Check all queues in the transcoding pipeline with proper locking.
    // Task() thread modifies these queues, so we need synchronization.
    // Use const_cast since this is a const method but we need to acquire the mutex.
    CCodecStream *pThis = const_cast<CCodecStream *>(this);

    pThis->Lock();
    bool result = empty() && m_LocalQueue.empty() && m_JitterBuffer.empty();
    pThis->Unlock();

    // Also check for packets in-flight between jitter buffer and stream push
    // (popped from jitter buffer under codec lock, not yet pushed to stream)
    if ( result && m_iInFlightPackets.load() > 0 )
    {
        result = false;
    }

    // CPacketStream::IsEmpty() checks its own queue before calling us,
    // so we only need to report on the transcoder pipeline queues
    return result;
}

////////////////////////////////////////////////////////////////////////////////////////
// thread

void CCodecStream::Thread(CCodecStream *This)
{
    // Signal that the thread is running and ready to process packets.
    // Init() spin-waits on this flag before returning, ensuring no packets
    // bypass the transcoder due to a race between thread startup and
    // incoming voice frames (e.g. NXDN's 4-frame batches).
    This->m_bConnected = true;

    while ( !This->m_bStopThread )
    {
        This->Task();
    }
}

void CCodecStream::Task(void)
{
    CBuffer Buffer;
    CIp     Ip;
    uint8   Ambe1[AMBE_FRAME_SIZE];
    uint8   Ambe2[AMBE_FRAME_SIZE];
    uint8   Ambe3[IMBE_SIZE];
    uint8   DStarSync[] = { 0x55,0x2D,0x16 };

    // Phase 1: Receive transcoded response from AMBEd
    if ( m_Socket.Receive(&Buffer, &Ip, 5) != -1 )
    {
        if ( IsValidAmbePacket(Buffer, Ambe1, Ambe2, Ambe3) )
        {
            m_TimeoutTimer.Now();

            // update statistics
            double ping = m_StatsTimer.DurationSinceNow();
            if ( m_fPingMin == -1 )
            {
                m_fPingMin = ping;
                m_fPingMax = ping;
            }
            else
            {
                m_fPingMin = MIN(m_fPingMin, ping);
                m_fPingMax = MAX(m_fPingMax, ping);
            }
            m_fPingSum += ping;
            m_fPingCount += 1;

            // Pop original packet from local queue and update with transcoded codecs
            // Lock protects m_LocalQueue and m_JitterBuffer against IsEmpty() reads
            Lock();
            if ( !m_LocalQueue.empty() )
            {
                CDvFramePacket *Packet = (CDvFramePacket *)m_LocalQueue.front();
                m_LocalQueue.pop();

                Packet->SetAmbe(m_uiCodecOut1, Ambe1);
                Packet->SetAmbe(m_uiCodecOut2, Ambe2);
                Packet->SetAmbe(m_uiCodecOut3, Ambe3);

                if ( (m_uiCodecOut1 == CODEC_AMBEPLUS || m_uiCodecOut2 == CODEC_AMBEPLUS) &&
                     (Packet->GetPacketId() % 21) == 0 )
                {
                    Packet->SetDvData(DStarSync);
                }

                m_JitterBuffer.push(Packet);

                if ( !m_bJitterBufferStarted )
                {
                    m_NextReleaseTime = std::chrono::steady_clock::now() +
                                        std::chrono::milliseconds(JITTER_BUFFER_DELAY_MS);
                    m_bJitterBufferStarted = true;
                }
            }
            Unlock();
        }
    }

    // Phase 2: Release from jitter buffer at regular 20ms intervals
    // Collect packets under lock, then push to packet stream without lock
    // to avoid CCodecStream::Lock() -> CPacketStream::Lock() ordering issue.
    // Track in-flight count so IsEmpty() knows packets are being transferred.
    std::vector<CPacket *> toRelease;
    Lock();
    if ( m_bJitterBufferStarted && !m_JitterBuffer.empty() )
    {
        auto now = std::chrono::steady_clock::now();
        // Cap at 3 releases per call to prevent bursting on catch-up
        int released = 0;
        while ( !m_JitterBuffer.empty() && now >= m_NextReleaseTime && released < 3 )
        {
            toRelease.push_back(m_JitterBuffer.front());
            m_JitterBuffer.pop();
            m_uiReturnedPackets++;
            m_NextReleaseTime += std::chrono::milliseconds(JITTER_BUFFER_FRAME_MS);
            released++;
        }
        // Mark packets as in-flight before releasing the lock
        m_iInFlightPackets += (int)toRelease.size();
    }
    Unlock();

    // Push to packet stream without holding CCodecStream lock
    for ( CPacket *p : toRelease )
    {
        m_PacketStream->Lock();
        m_PacketStream->push(p);
        m_PacketStream->Unlock();
        m_iInFlightPackets--;
    }

    // Phase 3: Drain main queue (filled by CPacketStream::Push) and send to AMBEd
    // Lock protects against concurrent push from router thread
    std::vector<CPacket *> toSend;
    Lock();
    while ( !empty() )
    {
        toSend.push_back(front());
        pop();
    }
    Unlock();

    for ( CPacket *Packet : toSend )
    {
        m_StatsTimer.Now();
        m_uiTotalPackets++;

        // Push to local queue BEFORE sending so the response handler
        // always finds the packet even if AMBEd responds immediately
        Lock();
        m_LocalQueue.push(Packet);
        Unlock();

        const uint8 *ambeData = ((CDvFramePacket *)Packet)->GetAmbe(m_uiCodecIn);
        if ( ambeData != NULL )
        {
            EncodeAmbePacket(&Buffer, ambeData);
            m_Socket.Send(Buffer, m_Ip, m_uiPort);
        }
    }

    // Phase 4: Timeout tracking — count slow AMBEd responses for stats.
    // Do NOT delete packets or disable the stream here. AMBEd will eventually
    // respond, and if it's truly dead, CloseStream's 2000ms drain timeout
    // provides the safety net (destructor cleans up remaining packets).
    if ( !m_LocalQueue.empty() && (m_TimeoutTimer.DurationSinceNow() >= (TRANSCODER_AMBEPACKET_TIMEOUT/1000.0f)) )
    {
        m_uiTimeoutPackets++;
        m_TimeoutTimer.Now();
    }
}

////////////////////////////////////////////////////////////////////////////////////////
/// packet decoding helpers

bool CCodecStream::IsValidAmbePacket(const CBuffer &Buffer, uint8 *Ambe1, uint8 *Ambe2, uint8 *Ambe3)
{
    bool valid = false;

    // New 3-codec response format:
    // codec1(1) + pid(1) + ambe1(9) + codec2(1) + codec2_data(8) + codec3(1) + imbe(11) = 32 bytes
    if ( Buffer.size() == TRANSCODER_PACKET_SIZE_3CODEC )
    {
        uint8 codec1 = Buffer.data()[TRANSCODER_PACKET_CODEC_OFFSET];
        uint8 codec2 = Buffer.data()[TRANSCODER_PACKET_CODEC2_OFFSET];
        uint8 codec3 = Buffer.data()[TRANSCODER_PACKET_CODEC3_OFFSET];

        // verify codecs match what we requested
        if ( codec1 == m_uiCodecOut1 && codec2 == m_uiCodecOut2 && codec3 == m_uiCodecOut3 )
        {
            // first output is 9 bytes (AMBE+ or AMBE2+)
            ::memcpy(Ambe1, &(Buffer.data()[TRANSCODER_PACKET_AMBE1_OFFSET]), AMBE_FRAME_SIZE);
            // second output is 8 bytes (Codec2)
            ::memcpy(Ambe2, &(Buffer.data()[TRANSCODER_PACKET_AMBE2_OFFSET]), CODEC2_FRAME_SIZE);
            // third output is 11 bytes (IMBE)
            ::memcpy(Ambe3, &(Buffer.data()[TRANSCODER_PACKET_AMBE3_OFFSET]), IMBE_SIZE);
            valid = true;
        }
    }
    // IMBE input response: codec1(1) + pid(1) + ambe1(9) + codec2(1) + ambe2(9) + codec3(1) + codec2_data(8) = 30 bytes
    // Outputs are AMBE+, AMBE2+, and Codec2 for D-Star, DMR, M17
    // Note: IMBEIN has different offsets than 3CODEC because ambe2 is 9 bytes (vs 8 byte codec2_data in 3CODEC)
    //   IMBEIN: codec3 at offset 21, codec2_data at offset 22
    //   3CODEC: codec3 at offset 20, imbe at offset 21
    else if ( Buffer.size() == TRANSCODER_PACKET_SIZE_IMBEIN )
    {
        uint8 codec1 = Buffer.data()[TRANSCODER_PACKET_CODEC_OFFSET];
        uint8 codec2 = Buffer.data()[TRANSCODER_PACKET_CODEC2_OFFSET];
        uint8 codec3 = Buffer.data()[21];  // IMBEIN: codec3 at offset 21 (after 9-byte ambe2)

        // verify codecs match what we requested
        if ( codec1 == m_uiCodecOut1 && codec2 == m_uiCodecOut2 && codec3 == m_uiCodecOut3 )
        {
            // first output is 9 bytes (AMBE+)
            ::memcpy(Ambe1, &(Buffer.data()[TRANSCODER_PACKET_AMBE1_OFFSET]), AMBE_FRAME_SIZE);
            // second output is 9 bytes (AMBE2+)
            ::memcpy(Ambe2, &(Buffer.data()[TRANSCODER_PACKET_AMBE2_OFFSET]), AMBE_FRAME_SIZE);
            // third output is 8 bytes (Codec2) at offset 22
            ::memcpy(Ambe3, &(Buffer.data()[22]), CODEC2_FRAME_SIZE);
            valid = true;
        }
    }
    // Codec2 input 3-codec response: codec1(1) + pid(1) + ambe1(9) + codec2(1) + ambe2(9) + codec3(1) + imbe(11) = 33 bytes
    // Outputs are AMBE+, AMBE2+, and IMBE for D-Star, DMR, P25
    else if ( Buffer.size() == TRANSCODER_PACKET_SIZE_C2IN_3CODEC )
    {
        uint8 codec1 = Buffer.data()[TRANSCODER_PACKET_CODEC_OFFSET];
        uint8 codec2 = Buffer.data()[TRANSCODER_PACKET_CODEC2_OFFSET];
        uint8 codec3 = Buffer.data()[21];  // C2IN_3CODEC: codec3 at offset 21 (after 9-byte ambe2)

        // verify codecs match what we requested
        if ( codec1 == m_uiCodecOut1 && codec2 == m_uiCodecOut2 && codec3 == m_uiCodecOut3 )
        {
            // first output is 9 bytes (AMBE+)
            ::memcpy(Ambe1, &(Buffer.data()[TRANSCODER_PACKET_AMBE1_OFFSET]), AMBE_FRAME_SIZE);
            // second output is 9 bytes (AMBE2+)
            ::memcpy(Ambe2, &(Buffer.data()[TRANSCODER_PACKET_AMBE2_OFFSET]), AMBE_FRAME_SIZE);
            // third output is 11 bytes (IMBE) at offset 22
            ::memcpy(Ambe3, &(Buffer.data()[22]), IMBE_SIZE);
            valid = true;
        }
    }
    // Codec2 input 2-codec response (legacy): codec1(1) + pid(1) + ambe1(9) + codec2(1) + ambe2(9) = 21 bytes
    // Both outputs are AMBE codecs (AMBE+ and AMBE2+)
    else if ( Buffer.size() == TRANSCODER_PACKET_SIZE_C2IN )
    {
        uint8 codec1 = Buffer.data()[TRANSCODER_PACKET_CODEC_OFFSET];
        uint8 codec2 = Buffer.data()[TRANSCODER_PACKET_CODEC2_OFFSET];

        // verify codecs match what we requested
        if ( codec1 == m_uiCodecOut1 && codec2 == m_uiCodecOut2 )
        {
            // first output is 9 bytes (AMBE+ or AMBE2+)
            ::memcpy(Ambe1, &(Buffer.data()[TRANSCODER_PACKET_AMBE1_OFFSET]), AMBE_FRAME_SIZE);
            // second output is also 9 bytes (AMBE+ or AMBE2+)
            ::memcpy(Ambe2, &(Buffer.data()[TRANSCODER_PACKET_AMBE2_OFFSET]), AMBE_FRAME_SIZE);
            // no third codec
            ::memset(Ambe3, 0, IMBE_SIZE);
            valid = true;
        }
    }
    // AMBE input response: codec1(1) + pid(1) + ambe1(9) + codec2(1) + codec2(8) = 20 bytes
    // First output is AMBE, second output is Codec2
    else if ( Buffer.size() == TRANSCODER_PACKET_SIZE_MULTI )
    {
        uint8 codec1 = Buffer.data()[TRANSCODER_PACKET_CODEC_OFFSET];
        uint8 codec2 = Buffer.data()[TRANSCODER_PACKET_CODEC2_OFFSET];

        // verify codecs match what we requested
        if ( codec1 == m_uiCodecOut1 && codec2 == m_uiCodecOut2 )
        {
            // first output is 9 bytes (AMBE+ or AMBE2+)
            ::memcpy(Ambe1, &(Buffer.data()[TRANSCODER_PACKET_AMBE1_OFFSET]), AMBE_FRAME_SIZE);
            // second output is 8 bytes (Codec2)
            ::memcpy(Ambe2, &(Buffer.data()[TRANSCODER_PACKET_AMBE2_OFFSET]), CODEC2_FRAME_SIZE);
            // no third codec
            ::memset(Ambe3, 0, IMBE_SIZE);
            valid = true;
        }
    }
    // Legacy single-codec response for backward compatibility
    else if ( Buffer.size() == TRANSCODER_PACKET_SIZE_LEGACY )
    {
        uint8 codec = Buffer.data()[TRANSCODER_PACKET_CODEC_OFFSET];
        if ( codec == m_uiCodecOut1 )
        {
            ::memcpy(Ambe1, &(Buffer.data()[TRANSCODER_PACKET_AMBE1_OFFSET]), AMBE_FRAME_SIZE);
            ::memset(Ambe2, 0, CODEC2_FRAME_SIZE);
            ::memset(Ambe3, 0, IMBE_SIZE);
            valid = true;
        }
    }
    return valid;
}

////////////////////////////////////////////////////////////////////////////////////////
/// packet encoding helpers

void CCodecStream::EncodeAmbePacket(CBuffer *Buffer, const uint8 *Ambe)
{
    Buffer->clear();
    Buffer->Append(m_uiCodecIn);
    Buffer->Append(m_uiPid);

    // Use correct size based on input codec
    if ( m_uiCodecIn == CODEC_CODEC2 )
    {
        Buffer->Append((uint8 *)Ambe, CODEC2_FRAME_SIZE);  // 8 bytes for Codec2
    }
    else if ( m_uiCodecIn == CODEC_IMBE )
    {
        Buffer->Append((uint8 *)Ambe, IMBE_SIZE);          // 11 bytes for IMBE
    }
    else
    {
        Buffer->Append((uint8 *)Ambe, AMBE_FRAME_SIZE);    // 9 bytes for AMBE
    }

    // increment PID for next packet (wraps at 256)
    m_uiPid++;
}
