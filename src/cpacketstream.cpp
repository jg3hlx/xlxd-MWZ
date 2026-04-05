//
//  cpacketstream.cpp
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

#include "main.h"
#include "cpacketstream.h"

////////////////////////////////////////////////////////////////////////////////////////
// constructor

CPacketStream::CPacketStream()
{
    m_bOpen = false;
    m_uiStreamId = 0;
    m_uiPacketCntr = 0;
    m_uiInputCodec = CODEC_NONE;
    m_OwnerClient = NULL;
    m_bHasCachedOwnerIp = false;
    m_CodecStream = NULL;
}

////////////////////////////////////////////////////////////////////////////////////////
// open / close

bool CPacketStream::Open(const CDvHeaderPacket &DvHeader, CClient *client)
{
    bool ok = false;

    // not already open?
    if ( !m_bOpen )
    {
        // update status
        m_bOpen = true;
        m_uiStreamId = DvHeader.GetStreamId();
        m_uiPacketCntr = 0;
        m_DvHeader = DvHeader;
        m_OwnerClient = client;
        m_CachedOwnerIp = client->GetIp();
        m_bHasCachedOwnerIp = true;
        m_LastPacketTime.Now();

        // get transcoder stream for cross-mode support
        // all codec types (D-Star, DMR, M17) transcode to both other codecs
        // Store the codec at open time to avoid client lifetime issues
        m_uiInputCodec = client->GetCodec();
        m_CodecStream = g_Transcoder.GetStream(this, m_uiInputCodec);
        if ( m_CodecStream == NULL && m_uiInputCodec != CODEC_NONE )
        {
            std::cout << "Warning: stream opened without transcoding (ambed unavailable) — same-mode only" << std::endl;
        }
        ok = true;
    }
    return ok;
}

void CPacketStream::Close(void)
{
    // update status
    m_bOpen = false;
    m_uiStreamId = 0;
    m_OwnerClient = NULL;
    m_bHasCachedOwnerIp = false;
    g_Transcoder.ReleaseStream(m_CodecStream);
    m_CodecStream = NULL;
}

////////////////////////////////////////////////////////////////////////////////////////
// push & pop

void CPacketStream::Push(CPacket *Packet)
{
    // update stream dependent packet data
    m_LastPacketTime.Now();
    Packet->UpdatePids(m_uiPacketCntr);
    if (Packet->IsDvFrame()) { m_uiPacketCntr++; }

    // Use transcoder for cross-codec conversion (D-Star, DMR, M17)
    // Each input codec transcodes to both other codecs in a combined response
    if ( m_CodecStream != NULL )
    {
        m_CodecStream->Lock();
        {
            // transcoder ready & frame need transcoding ?
            if ( m_CodecStream->IsConnected() && Packet->HaveTranscodableAmbe() )
            {
                // yes, push packet to trancoder queue
                // trancoder will push it after transcoding
                // is completed
                m_CodecStream->push(Packet);
            }
            else
            {
                // no, just bypass transcoder
                push(Packet);
            }
        }
        m_CodecStream->Unlock();
    }
    else
    {
        // No transcoder, push directly
        push(Packet);
    }
}

bool CPacketStream::IsEmpty(void)
{
    // Check codec stream pipeline first (acquires codec lock internally,
    // does NOT need the stream lock — avoids ABBA with Task() Phase 2
    // which holds codec lock then acquires stream lock).
    if ( m_CodecStream != NULL && !m_CodecStream->IsEmpty() )
    {
        return false;
    }

    // Codec pipeline is empty (or no transcoder). Now check the stream's
    // own queue under the stream lock. At this point no more packets can
    // arrive from the codec thread (its queues + in-flight are all empty),
    // so the lock acquire is safe — no ABBA since we don't hold the codec lock.
    Lock();
    bool bEmpty = empty();
    Unlock();

    return bEmpty;
}

////////////////////////////////////////////////////////////////////////////////////////
// get

const CIp *CPacketStream::GetOwnerIp(void)
{
    if ( m_bHasCachedOwnerIp )
    {
        return &m_CachedOwnerIp;
    }
    return NULL;
}

