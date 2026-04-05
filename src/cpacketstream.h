//
//  cpacketstream.h
//  xlxd
//
//  Created by Jean-Luc Deltombe (LX3JL) on 06/11/2015.
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

#ifndef cpacketstream_h
#define cpacketstream_h

#include <atomic>
#include "cpacketqueue.h"
#include "ctimepoint.h"
#include "cdvheaderpacket.h"
#include "ctranscoder.h"

////////////////////////////////////////////////////////////////////////////////////////

//#define STREAM_TIMEOUT      (0.600)
#define STREAM_TIMEOUT      (1.600)

////////////////////////////////////////////////////////////////////////////////////////
// class

class CPacketStream : public CPacketQueue
{
public:
    // constructor
    CPacketStream();
    
    // destructor
    virtual ~CPacketStream() {};

    // open / close
    bool Open(const CDvHeaderPacket &, CClient *);
    void Close(void);
    
    // push & pop
    void Push(CPacket *);
    void Tickle(void)                               { m_LastPacketTime.Now(); }
    bool IsEmpty(void);
    
    // get
    CClient         *GetOwnerClient(void)           { return m_OwnerClient; }
    void            SetOwnerClient(CClient *c)     { m_OwnerClient = c; }
    const CIp       *GetOwnerIp(void);
    bool            IsExpired(void) const           { return (m_LastPacketTime.DurationSinceNow() > STREAM_TIMEOUT); }
    bool            IsOpen(void) const              { return m_bOpen; }
    uint16          GetStreamId(void) const         { return m_uiStreamId; }
    const CCallsign &GetUserCallsign(void) const    { return m_DvHeader.GetMyCallsign(); }
    uint32          GetPacketCount(void) const      { return m_uiPacketCntr; }
    uint8           GetInputCodec(void) const       { return m_uiInputCodec; }

protected:
    // data
    std::atomic<bool>   m_bOpen;
    std::atomic<uint16> m_uiStreamId;
    uint32              m_uiPacketCntr;
    uint8               m_uiInputCodec;
    CClient             *m_OwnerClient;
    CIp                 m_CachedOwnerIp;
    std::atomic<bool>   m_bHasCachedOwnerIp;
    CTimePoint          m_LastPacketTime;
    CDvHeaderPacket     m_DvHeader;
    CCodecStream        *m_CodecStream;
};

////////////////////////////////////////////////////////////////////////////////////////
#endif /* cpacketstream_h */
