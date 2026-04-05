//
//  cprotocol.cpp
//  xlxd
//
//  Created by Jean-Luc Deltombe (LX3JL) on 01/11/2015.
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
#include "cprotocol.h"
#include "cclients.h"
#include "creflector.h"


////////////////////////////////////////////////////////////////////////////////////////
// constructor


CProtocol::CProtocol()
{
    m_bStopThread = false;
    m_bStopTxThread = false;
    m_pThread = NULL;
    m_pTxThread = NULL;
    m_Streams.reserve(NB_OF_MODULES);
}


////////////////////////////////////////////////////////////////////////////////////////
// destructor

CProtocol::~CProtocol()
{
    // kill RX thread
    m_bStopThread = true;
    if ( m_pThread != NULL )
    {
        m_pThread->join();
        delete m_pThread;
    }

    // kill TX thread
    m_bStopTxThread = true;
    m_QueueCondVar.notify_one();
    if ( m_pTxThread != NULL )
    {
        m_pTxThread->join();
        delete m_pTxThread;
    }

    // empty queue — delete any remaining packets to prevent leak on shutdown
    m_Queue.Lock();
    while ( !m_Queue.empty() )
    {
        delete m_Queue.front();
        m_Queue.pop();
    }
    m_Queue.Unlock();
}

////////////////////////////////////////////////////////////////////////////////////////
// initialization

bool CProtocol::Init(void)
{
    // init reflector apparent callsign
    m_ReflectorCallsign = g_Reflector.GetCallsign();

    // reset stop flags
    m_bStopThread = false;
    m_bStopTxThread = false;

    // start RX thread (or legacy single thread for unconverted protocols)
    m_pThread = new std::thread(CProtocol::RxThread, this);

    // start TX thread if protocol uses split-thread mode
    if ( UsesSplitThreads() )
    {
        m_pTxThread = new std::thread(CProtocol::TxThread, this);
    }

    // done
    return true;
}

void CProtocol::Close(void)
{
    // stop RX first — no new data enters the system
    m_bStopThread = true;
    if ( m_pThread != NULL )
    {
        m_pThread->join();
        delete m_pThread;
        m_pThread = NULL;
    }

    // then stop TX — drain remaining packets and exit
    m_bStopTxThread = true;
    m_QueueCondVar.notify_one();
    if ( m_pTxThread != NULL )
    {
        m_pTxThread->join();
        delete m_pTxThread;
        m_pTxThread = NULL;
    }
}


////////////////////////////////////////////////////////////////////////////////////////
// threads

void CProtocol::RxThread(CProtocol *This)
{
    if ( This->UsesSplitThreads() )
    {
        while ( !This->m_bStopThread )
        {
            This->RxTask();
        }
    }
    else
    {
        // legacy single-thread mode — run Task() which does both RX and TX
        while ( !This->m_bStopThread )
        {
            This->Task();
        }
    }
}

void CProtocol::TxThread(CProtocol *This)
{
    while ( !This->m_bStopTxThread )
    {
        This->TxTask();
    }

    // final drain — process any packets remaining after stop
    bool drained = false;
    while ( !drained )
    {
        This->HandleQueue();
        This->m_Queue.Lock();
        drained = This->m_Queue.empty();
        This->m_Queue.Unlock();
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// default TX task — condition variable wait + HandleQueue

void CProtocol::TxTask(void)
{
    // wait for queue notification or 20ms timeout
    {
        std::unique_lock<std::mutex> lk(m_QueueCondMutex);
        m_QueueCondVar.wait_for(lk, std::chrono::milliseconds(20));
    }

    HandleQueue();
}

////////////////////////////////////////////////////////////////////////////////////////
// packet encoding helpers

bool CProtocol::EncodeDvPacket(const CPacket &packet, CBuffer *buffer) const
{
    bool ok = false;
    if ( packet.IsDvFrame() )
    {
        if ( packet.IsLastPacket() )
        {
            ok = EncodeDvLastFramePacket((const CDvLastFramePacket &)packet, buffer);
        }
        else
        {
            ok = EncodeDvFramePacket((const CDvFramePacket &)packet, buffer);
        }
    }
    else if ( packet.IsDvHeader() )
    {
        ok = EncodeDvHeaderPacket((const CDvHeaderPacket &)packet, buffer);
    }
    else
    {
        buffer->clear();
    }
    return ok;
}

////////////////////////////////////////////////////////////////////////////////////////
// streams helpers

void CProtocol::OnDvFramePacketIn(CDvFramePacket *Frame, const CIp *Ip)
{
    // find the stream
    CPacketStream *stream = GetStream(Frame->GetStreamId(), Ip);
    if ( stream == NULL )
    {
        // try late entry buffering
        if ( !g_Reflector.BufferPendingFrame(Frame->GetStreamId(), Frame) )
        {
            delete Frame;
        }
    }
    else
    {
        // and push
        stream->Lock();
        stream->Push(Frame);
        stream->Unlock();
    }
}

void CProtocol::OnDvLastFramePacketIn(CDvLastFramePacket *Frame, const CIp *Ip)
{
    // find the stream
    CPacketStream *stream = GetStream(Frame->GetStreamId(), Ip);
    if ( stream == NULL )
    {
        // check if this is for a promoted late-entry stream
        stream = g_Reflector.GetPromotedStream(Frame->GetStreamId());
        if ( stream != NULL )
        {
            // handle last frame and close the promoted stream
            HandleStreamLastFrame(stream, Frame);
            g_Reflector.CloseStream(stream);
        }
        else
        {
            // check if pending (not yet promoted) — cancel it
            g_Reflector.CancelPendingEntry(Frame->GetStreamId());
            delete Frame;
        }
    }
    else
    {
        HandleStreamLastFrame(stream, Frame);
        g_Reflector.CloseStream(stream);

        // remove from our local stream cache to prevent stale entries
        // that could cause false Tickle matches on future streams
        for ( int i = 0; i < m_Streams.size(); i++ )
        {
            if ( m_Streams[i] == stream )
            {
                m_Streams.erase(m_Streams.begin()+i);
                break;
            }
        }
    }
}

void CProtocol::HandleStreamLastFrame(CPacketStream *stream, CDvLastFramePacket *Frame)
{
    // Push the last frame into the stream for transcoding and routing.
    // The drain wait is handled by CloseStream (called by the caller after this
    // returns) to avoid an ABBA deadlock: the drain loop would hold the stream
    // lock while calling CCodecStream::IsEmpty (which acquires the codec lock),
    // while the codec thread holds its lock and tries to acquire the stream lock.
    stream->Lock();
    stream->Push(Frame);
    stream->Unlock();
}

////////////////////////////////////////////////////////////////////////////////////////
// stream handle helpers

CPacketStream *CProtocol::GetStream(uint16 uiStreamId, const CIp *Ip)
{
    CPacketStream *stream = NULL;

    // find if we have a stream with same streamid in our cache
    for ( int i = 0; (i < m_Streams.size()) && (stream == NULL); i++ )
    {
        if ( m_Streams[i]->GetStreamId() == uiStreamId )
        {
            // if Ip is NULL, accept any stream with matching streamId
            // otherwise, also check if IP address matches (ignore port, as some
            // protocols like NXDN may send packets from different source ports)
            if ( Ip == NULL )
            {
                stream = m_Streams[i];
            }
            else if ( m_Streams[i]->GetOwnerIp() != NULL )
            {
                // Compare only the address part, not the port
                if ( Ip->GetAddr() == m_Streams[i]->GetOwnerIp()->GetAddr() )
                {
                    stream = m_Streams[i];
                }
            }
        }
    }
    // done
    return stream;
}

void CProtocol::CheckStreamsTimeout(void)
{
    for ( int i = 0; i < m_Streams.size(); i++ )
    {
        // time out ?
        m_Streams[i]->Lock();
        if ( m_Streams[i]->IsExpired() )
        {
            // yes, close it
            m_Streams[i]->Unlock();
            g_Reflector.CloseStream(m_Streams[i]);
            // and remove it
            m_Streams.erase(m_Streams.begin()+i);
            i--;
        }
        else
        {
            m_Streams[i]->Unlock();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// queue helper

void CProtocol::HandleQueue(void)
{
    // the default protocol just keep queue empty
    m_Queue.Lock();
    while ( !m_Queue.empty() )
    {
        delete m_Queue.front();
        m_Queue.pop();
    }
    m_Queue.Unlock();
}

////////////////////////////////////////////////////////////////////////////////////////
// syntax helper

bool CProtocol::IsNumber(char c) const
{
    return ((c >= '0') && (c <= '9'));
}

bool CProtocol::IsLetter(char c) const
{
    return ((c >= 'A') && (c <= 'Z'));
}

bool CProtocol::IsSpace(char c) const
{
    return (c == ' ');
}

////////////////////////////////////////////////////////////////////////////////////////
// DestId to Module helper

char CProtocol::DmrDstIdToModule(uint32 tg) const
{
    if ( tg >= 1 && tg <= (uint32)NB_OF_MODULES )
    {
        return ((char)(tg - 1) + 'A');
    }
    return ' ';
}

uint32 CProtocol::ModuleToDmrDestId(char m) const
{
    return (uint32)(m - 'A')+1;
}

