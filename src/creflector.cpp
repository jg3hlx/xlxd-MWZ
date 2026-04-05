//
//  creflector.cpp
//  xlxd
//
//  Created by Jean-Luc Deltombe (LX3JL) on 31/10/2015.
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
#include "creflector.h"
#include "cgatekeeper.h"
#include "cdmriddirfile.h"
#include "cdmriddirhttp.h"
#include "ctranscoder.h"
#include "cysfnodedirfile.h"
#include "cysfnodedirhttp.h"
#include "cnxdniddirfile.h"
#include "cnxdniddirhttp.h"

////////////////////////////////////////////////////////////////////////////////////////
// constructor

CReflector::CReflector()
{
    m_bStopThreads = false;
    m_XmlReportThread = NULL;
    m_JsonReportThread = NULL;
    for ( int i = 0; i < NB_OF_MODULES; i++ )
    {
        m_RouterThreads[i] = NULL;
    }
    ::memset(m_Mac, 0, sizeof(m_Mac));
#ifdef DEBUG_DUMPFILE
    m_DebugFile.open("/Users/jean-luc/Desktop/xlxdebug.txt");
#endif
}

CReflector::CReflector(const CCallsign &callsign)
{
#ifdef DEBUG_DUMPFILE
    m_DebugFile.close();
#endif
    m_bStopThreads = false;
    m_XmlReportThread = NULL;
    m_JsonReportThread = NULL;
    for ( int i = 0; i < NB_OF_MODULES; i++ )
    {
        m_RouterThreads[i] = NULL;
    }
    m_Callsign = callsign;
    ::memset(m_Mac, 0, sizeof(m_Mac));
}

////////////////////////////////////////////////////////////////////////////////////////
// destructor

CReflector::~CReflector()
{
    m_bStopThreads = true;
    if ( m_XmlReportThread != NULL )
    {
        m_XmlReportThread->join();
        delete m_XmlReportThread;
    }
    if ( m_JsonReportThread != NULL )
    {
        m_JsonReportThread->join();
        delete m_JsonReportThread;
    }
    for ( int i = 0; i < NB_OF_MODULES; i++ )
    {
        if ( m_RouterThreads[i] != NULL )
        {
             m_RouterThreads[i]->join();
             delete m_RouterThreads[i];
        }
    }
}


////////////////////////////////////////////////////////////////////////////////////////
// operation

bool CReflector::Start(void)
{
    bool ok = true;

    // reset stop flag
    m_bStopThreads = false;
    
    // init gate keeper
    ok &= g_GateKeeper.Init();
    
    // init dmrid directory
    g_DmridDir.Init();
    
    // init wiresx node directory
    g_YsfNodeDir.Init();

    // init nxdn id directory
    g_NxdnIdDir.Init();

    // init the transcoder
    g_Transcoder.Init();
    
    // create protocols
    ok &= m_Protocols.Init();
    
    // if ok, start threads
    if ( ok )
    {
        // start one thread per reflector module
        for ( int i = 0; i < NB_OF_MODULES; i++ )
        {
            m_RouterThreads[i] = new std::thread(CReflector::RouterThread, this, &(m_Streams[i]));
        }

        // start the reporting threads
        m_XmlReportThread = new std::thread(CReflector::XmlReportThread, this);
#ifdef JSON_MONITOR
        m_JsonReportThread = new std::thread(CReflector::JsonReportThread, this);
#endif
    }
    else
    {
        m_Protocols.Close();
    }
    
    // done
    return ok;
}

void CReflector::Stop(void)
{
    // stop & delete all threads
    m_bStopThreads = true;

    // stop & delete report threads
    if ( m_XmlReportThread != NULL )
    {
        m_XmlReportThread->join();
        delete m_XmlReportThread;
        m_XmlReportThread = NULL;
    }
    if ( m_JsonReportThread != NULL )
    {
        m_JsonReportThread->join();
        delete m_JsonReportThread;
        m_JsonReportThread = NULL;
    }

    // stop & delete all router thread
    for ( int i = 0; i < NB_OF_MODULES; i++ )
    {
        if ( m_RouterThreads[i] != NULL )
        {
            m_RouterThreads[i]->join();
            delete m_RouterThreads[i];
            m_RouterThreads[i] = NULL;
        }
    }

    // close protocols
    m_Protocols.Close();

    // close transcoder
    g_Transcoder.Close();
    
    // close gatekeeper
    g_GateKeeper.Close();
    
    // close databases
    g_DmridDir.Close();
    g_YsfNodeDir.Close();

}

////////////////////////////////////////////////////////////////////////////////////////
// stream opening & closing

CPacketStream *CReflector::OpenStream(CDvHeaderPacket *DvHeader, CClient *client)
{
    CPacketStream *retStream = NULL;
    
    // clients MUST have bee locked by the caller
    // so we can freely access it within the fuction
    
    // check sid is not NULL
    if ( DvHeader->GetStreamId() != 0 )
    {
        // check if client is valid candidate
        if ( m_Clients.IsClient(client) && !client->IsAMaster() )
        {
            // check if no stream with same streamid already open
            // to prevent loops
            if ( !IsStreamOpen(DvHeader) )
            {
                // get the module's queue
                char module = DvHeader->GetRpt2Module();
                CPacketStream *stream = GetStream(module);
                if ( stream != NULL )
                {
                    // lock it
                    stream->Lock();
                    // is it available ?
                    if ( stream->Open(*DvHeader, client) )
                    {
                        // stream open, mark client as master
                        // so that it can't be deleted
                        client->SetMasterOfModule(module);

                        // update last heard time
                        client->Heard();
                        retStream = stream;

                        // and push header packet
                        stream->Push(DvHeader);

                        // report
                        if ( client->IsPeer() )
                        {
                            std::cout << "Opening stream on module " << module << " for "
                                      << DvHeader->GetMyCallsign() << " via " << client->GetCallsign()
                                      << " with sid " << DvHeader->GetStreamId() << std::endl;
                        }
                        else
                        {
                            std::cout << "Opening stream on module " << module << " for "
                                      << DvHeader->GetMyCallsign() << " on " << client->GetCallsign()
                                      << " with sid " << DvHeader->GetStreamId() << std::endl;
                        }

                        // notify
                        g_Reflector.OnStreamOpen(stream->GetUserCallsign());

                    }
                    // unlock now
                    stream->Unlock();
                }
            }
            else
            {
                // report
                std::cout << "Detected stream loop on module " << DvHeader->GetRpt2Module() << " for client " << client->GetCallsign()
                          << " with sid " << DvHeader->GetStreamId() << std::endl;
            }
        }
    }
    
    // done
    return retStream;
}

void CReflector::CloseStream(CPacketStream *stream)
{
    //
    if ( stream != NULL )
    {
        // Wait for the transcoder pipeline to drain before closing.
        // Call IsEmpty() WITHOUT holding the stream lock to avoid ABBA deadlock:
        // the drain loop would hold stream lock then acquire codec lock (via
        // CCodecStream::IsEmpty), while CCodecStream::Task() holds codec lock
        // then acquires stream lock (for jitter buffer release).
        // The only writer to the stream queue at this point is CCodecStream::Task()
        // returning transcoded packets — no router traffic since we're at end of
        // transmission. A momentary read-race on the queue is benign: at worst we
        // poll one extra 10ms cycle.
        static const int MAX_DRAIN_WAIT_MS = 2000;  // 2 second max wait
        static const int DRAIN_POLL_MS = 10;
        int waitedMs = 0;
        bool bEmpty = false;
        do
        {
            bEmpty = stream->IsEmpty();
            if ( !bEmpty && waitedMs < MAX_DRAIN_WAIT_MS )
            {
                CTimePoint::TaskSleepFor(DRAIN_POLL_MS);
                waitedMs += DRAIN_POLL_MS;
            }
        } while (!bEmpty && waitedMs < MAX_DRAIN_WAIT_MS);

        if ( !bEmpty )
        {
            std::cout << "Warning: CloseStream drain timeout, some transcoded packets may be lost" << std::endl;
        }
        
        // lock clients
        GetClients();
        
        // lock stream
        stream->Lock();

        // get and check the master
        CClient *client = stream->GetOwnerClient();
        CClient *clientToDisconnect = NULL;
        if ( client != NULL )
        {
            // report to gatekeeper for loop detection
            char module = GetStreamModule(stream);
            bool shouldDisconnect = g_GateKeeper.ReportStreamClose(
                module,
                stream->GetUserCallsign(),
                stream->GetPacketCount(),
                client->IsPeer()
            );

            // client no longer a master
            client->NotAMaster();

            // notify
            g_Reflector.OnStreamClose(stream->GetUserCallsign());

            std::cout << "Closing stream of module " << module << std::endl;

            // mark for disconnect after stream is fully closed
            if ( shouldDisconnect && !client->IsPeer() )
            {
                clientToDisconnect = client;
            }

            // NULL the client pointer now (under stream lock) so no other
            // thread can dereference it after we release the clients lock.
            // GetOwnerIp() uses the cached copy set at Open() time.
            stream->SetOwnerClient(NULL);
        }

        // release clients
        ReleaseClients();

        // unlock before closing
        // to avoid double lock in associated
        // codecstream close/thread-join
        stream->Unlock();

        // close stream — resets state and releases transcoder stream
        stream->Close();

        // now safe to disconnect the looping client
        if ( clientToDisconnect != NULL )
        {
            GetClients();
            if ( m_Clients.IsClient(clientToDisconnect) )
            {
                std::cout << "Loop detection: removing client " << clientToDisconnect->GetCallsign() << std::endl;
                m_Clients.RemoveClient(clientToDisconnect);
            }
            ReleaseClients();
        }

        // check for pending late entry on this module
        int moduleIdx = -1;
        for ( int i = 0; i < (int)m_Streams.size(); i++ )
        {
            if ( &m_Streams[i] == stream )
            {
                moduleIdx = i;
                break;
            }
        }
        if ( moduleIdx >= 0 )
        {
            PromotePendingEntry(moduleIdx);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// late entry support

bool CReflector::TryLateEntry(CDvHeaderPacket *DvHeader, CClient *client)
{
    // clients MUST be locked by caller
    char module = DvHeader->GetRpt2Module();
    int idx = GetModuleIndex(module);
    if ( idx < 0 )
        return false;

    std::lock_guard<std::mutex> lock(m_PendingMutex[idx]);

    // only one pending entry per module
    if ( m_PendingEntries[idx].IsActive() )
    {
        return false;
    }

    // stash it
    m_PendingEntries[idx].Set(DvHeader, client);

    std::cout << "Late entry: stashed pending on module " << module
              << " for " << client->GetCallsign()
              << " with sid " << DvHeader->GetStreamId() << std::endl;

    return true;
}

bool CReflector::BufferPendingFrame(uint16 streamId, CPacket *frame)
{
    for ( int i = 0; i < NB_OF_MODULES; i++ )
    {
        std::lock_guard<std::mutex> lock(m_PendingMutex[i]);
        CPendingEntry &pending = m_PendingEntries[i];

        if ( !pending.IsActive() || pending.GetStreamId() != streamId )
            continue;

        // check expiry
        if ( pending.IsExpired() )
        {
            std::cout << "Late entry: pending expired on module " << GetModuleLetter(i) << std::endl;
            pending.Clear();
            return false;
        }

        // if promoted, forward directly to stream
        if ( pending.IsPromoted() )
        {
            CPacketStream *stream = pending.GetPromotedStream();
            stream->Lock();
            stream->Push(frame);
            stream->Unlock();
            return true;
        }

        // if buffering, try to buffer
        if ( pending.IsBuffering() )
        {
            if ( pending.BufferFrame(frame) )
            {
                return true;
            }
            // buffer full — BufferFrame already transitioned to HEADER_ONLY
            std::cout << "Late entry: buffer full on module " << GetModuleLetter(i) << ", switching to pure late entry" << std::endl;
            return false;
        }

        // HEADER_ONLY state — just drop the frame
        return false;
    }

    return false;
}

CPacketStream *CReflector::GetPromotedStream(uint16 streamId)
{
    for ( int i = 0; i < NB_OF_MODULES; i++ )
    {
        std::lock_guard<std::mutex> lock(m_PendingMutex[i]);
        CPendingEntry &pending = m_PendingEntries[i];

        if ( pending.IsPromoted() && pending.GetStreamId() == streamId )
        {
            return pending.GetPromotedStream();
        }
    }
    return NULL;
}

void CReflector::CancelPendingEntry(uint16 streamId)
{
    for ( int i = 0; i < NB_OF_MODULES; i++ )
    {
        std::lock_guard<std::mutex> lock(m_PendingMutex[i]);
        CPendingEntry &pending = m_PendingEntries[i];

        if ( pending.IsActive() && pending.GetStreamId() == streamId )
        {
            std::cout << "Late entry: cancelled pending on module " << GetModuleLetter(i) << std::endl;
            pending.Clear();
            return;
        }
    }
}

void CReflector::PromotePendingEntry(int moduleIdx)
{
    // Lock order: Clients THEN PendingMutex
    // TryLateEntry acquires Clients (held by caller) then PendingMutex,
    // so we must use the same order here to prevent ABBA deadlock.
    GetClients();
    std::lock_guard<std::mutex> lock(m_PendingMutex[moduleIdx]);
    CPendingEntry &pending = m_PendingEntries[moduleIdx];

    if ( !pending.IsActive() )
    {
        ReleaseClients();
        return;
    }

    if ( pending.IsExpired() )
    {
        std::cout << "Late entry: pending expired on module " << GetModuleLetter(moduleIdx) << std::endl;
        pending.Clear();
        ReleaseClients();
        return;
    }

    // re-validate client
    CClient *client = pending.GetClient();

    if ( !m_Clients.IsClient(client) || client->IsAMaster() )
    {
        std::cout << "Late entry: client no longer valid on module " << GetModuleLetter(moduleIdx) << std::endl;
        pending.Clear();
        ReleaseClients();
        return;
    }

    // try to open the stream
    CPacketStream *stream = &m_Streams[moduleIdx];
    stream->Lock();
    if ( stream->Open(*(pending.GetHeader()), client) )
    {
        // mark client as master
        client->SetMasterOfModule(GetModuleLetter(moduleIdx));
        client->Heard();

        // push a copy of the header to the stream
        stream->Push(new CDvHeaderPacket(*(pending.GetHeader())));

        stream->Unlock();

        std::cout << "Late entry: promoted on module " << GetModuleLetter(moduleIdx)
                  << " for " << client->GetCallsign() << std::endl;

        // notify
        OnStreamOpen(stream->GetUserCallsign());

        // replay buffered frames
        pending.ReplayInto(stream);

        // transition to promoted state for ongoing frame forwarding
        pending.Promote(stream);

        ReleaseClients();
    }
    else
    {
        // shouldn't happen — we just closed the stream
        stream->Unlock();
        std::cout << "Late entry: failed to open stream on module " << GetModuleLetter(moduleIdx) << std::endl;
        pending.Clear();
        ReleaseClients();
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// router threads

void CReflector::RouterThread(CReflector *This, CPacketStream *streamIn)
{
    // get our module
    uint8 uiModuleId = This->GetStreamModule(streamIn);

    // get on input queue
    CPacket *packet;

    while ( !This->m_bStopThreads )
    {
        // any packet in our input queue ?
        streamIn->Lock();
        if ( !streamIn->empty() )
        {
            // get the packet
            packet = streamIn->front();
            streamIn->pop();
        }
        else
        {
            packet = NULL;
        }
        streamIn->Unlock();

        // route it
        if ( packet != NULL )
        {
            // set origin
            packet->SetModuleId(uiModuleId);

            // iterate on all protocols
            for ( int i = 0; i < This->m_Protocols.Size(); i++ )
            {
                // duplicate packet
                CPacket *packetClone = packet->Duplicate();

                // get protocol
                CProtocol *protocol = This->m_Protocols.GetProtocol(i);

                // if packet is header, update RPT2 according to protocol
                if ( packetClone->IsDvHeader() )
                {
                    // get our callsign
                    CCallsign csRPT = protocol->GetReflectorCallsign();
                    csRPT.SetModule(This->GetStreamModule(streamIn));
                    ((CDvHeaderPacket *)packetClone)->SetRpt2Callsign(csRPT);
                }

                // and push it
                CPacketQueue *queue = protocol->GetQueue();
                queue->push(packetClone);
                protocol->ReleaseQueue();
            }

            // done
            delete packet;
            packet = NULL;
        }
        else
        {
            // Safety net: if stream is open but expired (no packets for
            // STREAM_TIMEOUT), close it. This catches promoted late-entry
            // streams that aren't tracked in any protocol's m_Streams.
            streamIn->Lock();
            bool orphaned = streamIn->IsOpen() && streamIn->IsExpired();
            streamIn->Unlock();
            if ( orphaned )
            {
                std::cout << "Router: closing expired stream on module " << (char)uiModuleId << std::endl;
                This->CloseStream(streamIn);
            }
            else
            {
                // wait a bit
                CTimePoint::TaskSleepFor(10);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// report threads

void CReflector::XmlReportThread(CReflector *This)
{
    while ( !This->m_bStopThreads )
    {
        // report to xml file
        std::ofstream xmlFile;
        xmlFile.open(XML_PATH, std::ios::out | std::ios::trunc);
        if ( xmlFile.is_open() )
        {
            // write xml file
            This->WriteXmlFile(xmlFile);

            // and close file
            xmlFile.close();
        }
#ifndef DEBUG_NO_ERROR_ON_XML_OPEN_FAIL
        else
        {
            std::cout << "Failed to open " << XML_PATH  << std::endl;
        }
#endif

        // and wait a bit
        CTimePoint::TaskSleepFor(XML_UPDATE_PERIOD * 1000);
    }
}

void CReflector::JsonReportThread(CReflector *This)
{
    CUdpSocket Socket;
    CBuffer    Buffer;
    CIp        Ip;
    bool       bOn;

    // init variable
    bOn = false;

    // create listening socket
    if ( Socket.Open(JSON_PORT) )
    {
        // and loop
        while ( !This->m_bStopThreads )
        {
            // any command ?
            if ( Socket.Receive(&Buffer, &Ip, 50) != -1 )
            {
                // check verb
                if ( Buffer.Compare((uint8 *)"hello", 5) == 0 )
                {
                    std::cout << "Monitor socket connected with " << Ip << std::endl;

                    // connected
                    bOn = true;

                    // announce ourselves
                    This->SendJsonReflectorObject(Socket, Ip);

					// dump tables
					This->SendJsonNodesObject(Socket, Ip);
					This->SendJsonStationsObject(Socket, Ip);
                }
                else if ( Buffer.Compare((uint8 *)"bye", 3) == 0 )
                {
                    std::cout << "Monitor socket disconnected" << std::endl;

                    // diconnected
                    bOn = false;
                }
            }

            // any notifications ?
            CNotification notification;
            This->m_Notifications.Lock();
            if ( !This->m_Notifications.empty() )
            {
                // get the packet
                notification = This->m_Notifications.front();
                This->m_Notifications.pop();
            }
            This->m_Notifications.Unlock();

            // handle it
            if ( bOn )
            {
                switch ( notification.GetId() )
                {
                    case NOTIFICATION_CLIENTS:
                    case NOTIFICATION_PEERS:
                        //std::cout << "Monitor updating nodes table" << std::endl;
                        This->SendJsonNodesObject(Socket, Ip);
                        break;
                    case NOTIFICATION_USERS:
                        //std::cout << "Monitor updating stations table" << std::endl;
                        This->SendJsonStationsObject(Socket, Ip);
                        break;
                    case NOTIFICATION_STREAM_OPEN:
                        //std::cout << "Monitor notify station " << notification.GetCallsign() << "going ON air" << std::endl;
                        This->SendJsonStationsObject(Socket, Ip);
                        This->SendJsonOnairObject(Socket, Ip, notification.GetCallsign());
                        break;
                    case NOTIFICATION_STREAM_CLOSE:
                        //std::cout << "Monitor notify station " << notification.GetCallsign() << "going OFF air" << std::endl;
                        This->SendJsonOffairObject(Socket, Ip, notification.GetCallsign());
                        break;
                   case NOTIFICATION_NONE:
                    default:
                        // nothing to do, just sleep a bit
                        CTimePoint::TaskSleepFor(250);
                        break;
                }
            }
        }
    }
    else
    {
        std::cout << "Error creating monitor socket" << std::endl;
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// notifications

void CReflector::OnPeersChanged(void)
{
    CNotification notification(NOTIFICATION_PEERS);
    
    m_Notifications.Lock();
    m_Notifications.push(notification);
    m_Notifications.Unlock();
}

void CReflector::OnClientsChanged(void)
{
    CNotification notification(NOTIFICATION_CLIENTS);

    m_Notifications.Lock();
    m_Notifications.push(notification);
    m_Notifications.Unlock();
}

void CReflector::OnUsersChanged(void)
{
    CNotification notification(NOTIFICATION_USERS);

    m_Notifications.Lock();
    m_Notifications.push(notification);
    m_Notifications.Unlock();
}

void CReflector::OnStreamOpen(const CCallsign &callsign)
{
    CNotification notification(NOTIFICATION_STREAM_OPEN, callsign);

    m_Notifications.Lock();
    m_Notifications.push(notification);
    m_Notifications.Unlock();
}

void CReflector::OnStreamClose(const CCallsign &callsign)
{
    CNotification notification(NOTIFICATION_STREAM_CLOSE, callsign);

    m_Notifications.Lock();
    m_Notifications.push(notification);
    m_Notifications.Unlock();
}

////////////////////////////////////////////////////////////////////////////////////////
// modules & queues

int CReflector::GetModuleIndex(char module) const
{
    int i = (int)module - (int)'A';
    if ( (i < 0) || (i >= NB_OF_MODULES) )
    {
        i = -1;
    }
    return i;
}

CPacketStream *CReflector::GetStream(char module)
{
    CPacketStream *stream = NULL;
    int i = GetModuleIndex(module);
    if ( i >= 0 )
    {
        stream = &(m_Streams[i]);
    }
    return stream;
}

void CReflector::ReleaseStreamOwner(CClient *client)
{
    // Null out the owner pointer on any stream owned by this client.
    // Must be called BEFORE deleting the client object to prevent
    // dangling pointer dereference in CloseStream/RouterThread.
    // Caller must hold the Clients lock.
    for ( int i = 0; i < m_Streams.size(); i++ )
    {
        m_Streams[i].Lock();
        if ( m_Streams[i].GetOwnerClient() == client )
        {
            client->NotAMaster();
            m_Streams[i].SetOwnerClient(NULL);
        }
        m_Streams[i].Unlock();
    }
}

bool CReflector::IsStreamOpen(const CDvHeaderPacket *DvHeader)
{
    bool open = false;
    for ( int i = 0; (i < m_Streams.size()) && !open; i++  )
    {
        open =  ( (m_Streams[i].GetStreamId() == DvHeader->GetStreamId()) &&
                  (m_Streams[i].IsOpen()));
    }
    return open;
}

char CReflector::GetStreamModule(CPacketStream *stream)
{
    char module = ' ';
    for ( int i = 0; (i < m_Streams.size()) && (module == ' '); i++ )
    {
        if ( &(m_Streams[i]) == stream )
        {
            module = GetModuleLetter(i);
        }
    }
    return module;
}

////////////////////////////////////////////////////////////////////////////////////////
// xml helpers

void CReflector::WriteXmlFile(std::ofstream &xmlFile)
{
    // write header
    xmlFile << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << std::endl;
    
    // software version
    char sz[64];
    ::sprintf(sz, "%d.%d.%d", VERSION_MAJOR, VERSION_MINOR, VERSION_REVISION);
    xmlFile << "<Version>" << sz << "</Version>" << std::endl;
    
    // linked peers
    xmlFile << "<" << m_Callsign << "linked peers>" << std::endl;
    // lock
    CPeers *peers = GetPeers();
    // iterate on peers
    for ( int i = 0; i < peers->GetSize(); i++ )
    {
        peers->GetPeer(i)->WriteXml(xmlFile);
    }
    // unlock
    ReleasePeers();
    xmlFile << "</" << m_Callsign << "linked peers>" << std::endl;
    
    // linked nodes
    xmlFile << "<" << m_Callsign << "linked nodes>" << std::endl;
    // lock
    CClients *clients = GetClients();
    // iterate on clients
    for ( int i = 0; i < clients->GetSize(); i++ )
    {
        if ( clients->GetClient(i)->IsNode() )
        {
            clients->GetClient(i)->WriteXml(xmlFile);
        }
    }
    // unlock
    ReleaseClients();
    xmlFile << "</" << m_Callsign << "linked nodes>" << std::endl;
    
    // last heard users
    xmlFile << "<" << m_Callsign << "heard users>" << std::endl;
    // lock
    CUsers *users = GetUsers();
    // iterate on users
    for ( int i = 0; i < users->GetSize(); i++ )
    {
        users->GetUser(i)->WriteXml(xmlFile);
    }
    // unlock
    ReleaseUsers();
    xmlFile << "</" << m_Callsign << "heard users>" << std::endl;
}

////////////////////////////////////////////////////////////////////////////////////////
// json helpers

void CReflector::SendJsonReflectorObject(CUdpSocket &Socket, CIp &Ip)
{
	char Buffer[1024];
 	char cs[CALLSIGN_LEN+1];
 	char mod[8] = "\"A\"";

 	// get reflector callsign
    m_Callsign.GetCallsign((uint8 *)cs);
    cs[CALLSIGN_LEN] = 0;

	// build string
	::sprintf(Buffer, "{\"reflector\":\"%s\",\"modules\":[", cs);
    for ( int i = 0; i < NB_OF_MODULES; i++ )
    {
    	::strcat(Buffer, mod);
    	mod[1]++;
        if ( i < NB_OF_MODULES-1 )
        {
        	::strcat(Buffer, ",");
        }
    }
    ::strcat(Buffer, "]}");

    // and send
    Socket.Send(Buffer, Ip);
}

#define JSON_NBMAX_NODES	250

void CReflector::SendJsonNodesObject(CUdpSocket &Socket, CIp &Ip)
{
	char Buffer[12+(JSON_NBMAX_NODES*94)];

    // nodes object table
    ::sprintf(Buffer, "{\"nodes\":[");
    // lock
    CClients *clients = GetClients();
    // iterate on clients
    for ( int i = 0; (i < clients->GetSize()) && (i < JSON_NBMAX_NODES); i++ )
    {
        clients->GetClient(i)->GetJsonObject(Buffer);
        if ( i < clients->GetSize()-1 )
        {
        	::strcat(Buffer, ",");
        }
    }
    // unlock
    ReleaseClients();
    ::strcat(Buffer, "]}");

    // and send
    //std::cout << Buffer << std::endl;
    Socket.Send(Buffer, Ip);
}

void CReflector::SendJsonStationsObject(CUdpSocket &Socket, CIp &Ip)
{
	char Buffer[15+(LASTHEARD_USERS_MAX_SIZE*94)];

    // stations object table
    ::sprintf(Buffer, "{\"stations\":[");

    // lock
    CUsers *users = GetUsers();
    // iterate on users
    for ( int i = 0; i < users->GetSize(); i++ )
    {
        users->GetUser(i)->GetJsonObject(Buffer);
        if ( i < users->GetSize()-1 )
        {
        	::strcat(Buffer, ",");
        }
    }
    // unlock
    ReleaseUsers();

    ::strcat(Buffer, "]}");

    // and send
    //std::cout << Buffer << std::endl;
    Socket.Send(Buffer, Ip);
}

void CReflector::SendJsonOnairObject(CUdpSocket &Socket, CIp &Ip, const CCallsign &Callsign)
{
    char Buffer[128];
    char sz[CALLSIGN_LEN+1];

    // onair object
    Callsign.GetCallsignString(sz);
    ::sprintf(Buffer, "{\"onair\":\"%s\"}", sz);

    // and send
    //std::cout << Buffer << std::endl;
    Socket.Send(Buffer, Ip);
}

void CReflector::SendJsonOffairObject(CUdpSocket &Socket, CIp &Ip, const CCallsign &Callsign)
{
    char Buffer[128];
    char sz[CALLSIGN_LEN+1];

    // offair object
    Callsign.GetCallsignString(sz);
    ::sprintf(Buffer, "{\"offair\":\"%s\"}", sz);

    // and send
    //std::cout << Buffer << std::endl;
    Socket.Send(Buffer, Ip);
}

////////////////////////////////////////////////////////////////////////////////////////
// MAC address helpers

#ifdef __linux__
#include <netpacket/packet.h>
bool CReflector::UpdateListenMac(void)
{
    struct ifaddrs *ifap, *ifaptr;
    char host[NI_MAXHOST];
    char *ifname = NULL;
    bool found = false;
    
    // iterate through all our AF_INET interface to find the one
    // of our listening ip
    if ( getifaddrs(&ifap) == 0 )
    {
        for ( ifaptr = ifap; (ifaptr != NULL) && !found; ifaptr = (ifaptr)->ifa_next )
        {
            // is it an AF_INET?
            if ( ifaptr->ifa_addr && ifaptr->ifa_addr->sa_family == AF_INET )
            {
                // get the IP
                if ( getnameinfo(ifaptr->ifa_addr,
                        sizeof(struct sockaddr_in),
                        host, NI_MAXHOST,
                        NULL, 0, NI_NUMERICHOST) == 0 )
                {
                    if ( CIp(host) == m_Ip )
                    {
                        // yes, found it
                        found = true;
                        ifname = new char[strlen(ifaptr->ifa_name)+1];
                        strcpy(ifname, ifaptr->ifa_name);
                    }
                }
            }
           
        }
        freeifaddrs(ifap);
    }
    
    // if listening interface name found, iterate again
    // to find the corresponding AF_PACKET interface
    if ( found )
    {
        found = false;
        if ( getifaddrs(&ifap) == 0 )
        {
            for ( ifaptr = ifap; (ifaptr != NULL) && !found; ifaptr = (ifaptr)->ifa_next )
            {
                if ( !strcmp((ifaptr)->ifa_name, ifname) && (ifaptr->ifa_addr->sa_family == AF_PACKET) )
                {
                    found = true;
                    struct sockaddr_ll *s = (struct sockaddr_ll *)(ifaptr->ifa_addr);
                    for ( int i = 0; i < 6; i++ )
                    {
                        m_Mac[i] = s->sll_addr[i];
                    }
                }
            }
        }
        freeifaddrs(ifap);
    }
    
    // done
    return found;
}
#endif

#if defined(__APPLE__)  && defined(__MACH__)
#include <net/if_dl.h>
bool CReflector::UpdateListenMac(void)
{
    struct ifaddrs *ifaddr;
    int  s;
    char host[NI_MAXHOST];
    char *ifname = NULL;
    bool found = false;
    bool ok = false;

    if ( getifaddrs(&ifaddr) != -1)
    {
        // Walk through linked list, maintaining head pointer so we can free list later.
        // until finding our listening AF_INET interface
        for (struct ifaddrs *ifa = ifaddr; (ifa != NULL) && !found; ifa = ifa->ifa_next)
        {
            if (ifa->ifa_addr == NULL)
                continue;

            // is it an AF_INET?
            if (ifa->ifa_addr->sa_family == AF_INET)
            {
                // get IP
                s = getnameinfo(ifa->ifa_addr,
                        sizeof(struct sockaddr_in),
                        host, NI_MAXHOST,
                        NULL, 0, NI_NUMERICHOST);
                if (s != 0)
                {
                   return false;
                }
                // is it our listening ip ?
                if ( CIp(host) == m_Ip )
                {
                    // yes, found it
                    found = true;
                    ifname = new char[strlen(ifa->ifa_name)+1];
                    strcpy(ifname, ifa->ifa_name);
                }
            }
        }
        freeifaddrs(ifaddr);

        // found our interface ?
        if ( found )
        {
            // yes
            //std::cout << ifname << " : " << host << std::endl;
            
            // Walk again through linked list
            // until finding our listening AF_LINK interface
            if ( getifaddrs(&ifaddr) != -1 )
            {
                found = false;
                for (struct ifaddrs *ifa = ifaddr; (ifa != NULL) && !found; ifa = ifa->ifa_next)
                {
                    if (ifa->ifa_addr == NULL)
                        continue;
                    
                    if ( !strcmp(ifa->ifa_name, ifname) && (ifa->ifa_addr->sa_family == AF_LINK))
                    {
                        ::memcpy((void *)m_Mac, (void *)LLADDR((struct sockaddr_dl *)(ifa)->ifa_addr), sizeof(m_Mac));
                        ok = true;
                        found = true;
                    }
                }
                freeifaddrs(ifaddr);
            }
        }

        delete [] ifname;
    }
    return ok;
}
#endif
