//
//  cvocodecchannel.h
//  ambed
//
//  Created by Jean-Luc Deltombe (LX3JL) on 23/04/2017.
//  Copyright © 2015 Jean-Luc Deltombe (LX3JL). All rights reserved.
//
// ----------------------------------------------------------------------------
//    This file is part of ambed.
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


#ifndef cvocodecchannel_h
#define cvocodecchannel_h

#include <mutex>
#include <queue>
#include <vector>
#include <thread>
#include <condition_variable>
#include <atomic>
#include "cpacketqueue.h"
#include "csignalprocessor.h"
#include "cvoicepacket.h"
#include "codec2/codec2.h"
#include "imbe/imbe_vocoder.h"

////////////////////////////////////////////////////////////////////////////////////////
// class

class CVocodecInterface;

class CVocodecChannel
{
public:
    // constructors
    CVocodecChannel(CVocodecInterface *, int, CVocodecInterface *, int, int);

    // destructor
    virtual ~CVocodecChannel();
    
    // open & close
    bool Open(void);
    bool IsOpen(void) const                 { return m_bOpen; }
    void Close(void);
    
    // get
    uint8 GetCodecIn(void) const;
    uint8 GetCodecOut(void) const;
    int   GetChannelIn(void) const          { return m_iChannelIn; }
    int   GetChannelOut(void) const         { return m_iChannelOut; }
    int   GetSpeechGain(void) const         { return m_iSpeechGain; }
    
    //Processing
    void ProcessSignal(CVoicePacket& voicePacket);
    
    // interfaces
    bool IsInterfaceIn(const CVocodecInterface *interface)      { return (interface == m_InterfaceIn); }
    bool IsInterfaceOut(const CVocodecInterface *interface)     { return (interface == m_InterfaceOut); }
    
    // queues
    CPacketQueue *GetPacketQueueIn(void)    { m_QueuePacketIn.Lock(); return &m_QueuePacketIn; }
    void ReleasePacketQueueIn(void)         { m_QueuePacketIn.Unlock(); }
    CPacketQueue *GetPacketQueueOut(void)   { m_QueuePacketOut.Lock(); return &m_QueuePacketOut; }
    void ReleasePacketQueueOut(void)        { m_QueuePacketOut.Unlock(); }
    CPacketQueue *GetVoiceQueue(void)       { m_QueueVoice.Lock(); return &m_QueueVoice; }
    void ReleaseVoiceQueue(void)            { m_QueueVoice.Unlock(); }

    // Codec2 queue (for software-encoded Codec2 data)
    void EnableCodec2(bool bEnable);
    bool HasCodec2Data(void);
    bool GetCodec2Data(uint8 *codec2);
    bool IsCodec2Enabled(void) const        { return m_bCodec2Enabled; }

    // IMBE queue (for software-encoded IMBE data)
    void EnableImbe(bool bEnable);
    bool HasImbeData(void);
    bool GetImbeData(uint8 *imbe);
    bool IsImbeEnabled(void) const          { return m_bImbeEnabled; }

    // operators
    //virtual bool operator ==(const CVocodecChannel &) const   { return false; }
    
protected:
    // queues helpers
    void PurgeAllQueues(void);
    
protected:
    // status
    std::atomic<bool>   m_bOpen;

    // connected interfaces
    CVocodecInterface   *m_InterfaceIn;
    int                 m_iChannelIn;
    CVocodecInterface   *m_InterfaceOut;
    int                 m_iChannelOut;
    
    // ambe queues
    CPacketQueue        m_QueuePacketIn;
    CPacketQueue        m_QueuePacketOut;
    // voice queue
    CPacketQueue        m_QueueVoice;
    
    // settings
    int                 m_iSpeechGain;

    // Codec2 encoding (software) - runs in separate thread to avoid blocking hardware
    std::atomic<bool>   m_bCodec2Enabled;
    CCodec2             *m_pCodec2;
    std::mutex          m_Codec2Mutex;
    std::queue<std::vector<uint8>> m_Codec2Queue;

    // Async Codec2 encoding thread
    std::thread         *m_pCodec2Thread;
    std::atomic<bool>   m_bCodec2StopThread;
    std::mutex          m_Codec2PcmMutex;
    std::condition_variable m_Codec2PcmCondition;
    std::queue<std::vector<int16_t>> m_Codec2PcmQueue;  // PCM samples waiting to be encoded
    static void Codec2EncoderThread(CVocodecChannel *pChannel);
    void Codec2EncoderTask(void);

    // IMBE encoding (software) - runs in separate thread to avoid blocking hardware
    std::atomic<bool>   m_bImbeEnabled;
    imbe_vocoder        *m_pImbe;
    std::mutex          m_ImbeMutex;
    std::queue<std::vector<uint8>> m_ImbeQueue;

    // Async IMBE encoding thread
    std::thread         *m_pImbeThread;
    std::atomic<bool>   m_bImbeStopThread;
    std::mutex          m_ImbePcmMutex;
    std::condition_variable m_ImbePcmCondition;
    std::queue<std::vector<int16_t>> m_ImbePcmQueue;  // PCM samples waiting to be encoded
    static void ImbeEncoderThread(CVocodecChannel *pChannel);
    void ImbeEncoderTask(void);

private:
    CSignalProcessor* m_signalProcessor;
};

////////////////////////////////////////////////////////////////////////////////////////
#endif /* cvocodecchannel_h */
