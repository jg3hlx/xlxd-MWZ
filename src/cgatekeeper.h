//
//  cgatekeeper.h
//  xlxd
//
//  Created by Jean-Luc Deltombe (LX3JL) on 07/11/2015.
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

#ifndef cgatekeeper_h
#define cgatekeeper_h

#include <atomic>
#include "main.h"
#include "ccallsign.h"
#include "cip.h"
#include "ccallsignlist.h"
#include "cpeercallsignlist.h"
#include "ctimepoint.h"
#include <mutex>
#include <map>

////////////////////////////////////////////////////////////////////////////////////////
// loop detection defines

#define LOOP_DETECT_GAP_SEC         2       // max seconds between close and reopen
#define LOOP_DETECT_SHORT_FRAMES    50      // max frames to consider "short" (~1 sec)
#define LOOP_DETECT_SHORT_COUNT     3       // consecutive short streams before strike
#define LOOP_BACKOFF_1_SEC          3       // first strike: 3 second TX block
#define LOOP_BACKOFF_2_SEC          10      // second strike: 10 second TX block
#define LOOP_BACKOFF_3_SEC          300     // third strike: 5 minute TX + link block
#define LOOP_STRIKE_DECAY_SEC       300     // strikes decay after 5 minutes clean

////////////////////////////////////////////////////////////////////////////////////////
// loop detection structures

struct CStreamHistory
{
    CCallsign   m_MyCallsign;
    CTimePoint  m_CloseTime;
    uint32      m_FrameCount;
    int         m_ConsecutiveShort;
    bool        m_bValid;

    CStreamHistory() : m_FrameCount(0), m_ConsecutiveShort(0), m_bValid(false) {}
};

struct CLoopPenalty
{
    int         m_StrikeCount;
    CTimePoint  m_LastStrikeTime;
    CTimePoint  m_BlockUntil;
    bool        m_bLinkBlocked;
    bool        m_bBlockLogged;

    CLoopPenalty() : m_StrikeCount(0), m_bLinkBlocked(false), m_bBlockLogged(false) {}
};

////////////////////////////////////////////////////////////////////////////////////////
// class

class CGateKeeper
{
public:
    // constructor
    CGateKeeper();

    // destructor
    virtual ~CGateKeeper();

    // init & clode
    bool Init(void);
    void Close(void);

    // authorizations
    bool MayLink(const CCallsign &, const CIp &, int, char * = NULL);
    bool MayTransmit(const CCallsign &, const CIp &, int = PROTOCOL_ANY, char = ' ');

    // peer list handeling
    CPeerCallsignList *GetPeerList(void)    { m_PeerList.Lock(); return &m_PeerList; }
    void ReleasePeerList(void)              { m_PeerList.Unlock(); }

    // loop detection - returns true if client should be disconnected (strike 3, non-peer only)
    bool ReportStreamClose(char module, const CCallsign &myCallsign, uint32 frameCount, bool isPeer);

protected:
    // thread
    static void Thread(CGateKeeper *);

    // operation helpers
    bool IsNodeListedOk(const CCallsign &, const CIp &, char = ' ') const;
    bool IsPeerListedOk(const CCallsign &, const CIp &, char) const;
    bool IsPeerListedOk(const CCallsign &, const CIp &, char *) const;

    // loop detection helpers
    bool IsLoopSuppressed(const CCallsign &myCallsign, char module);

protected:
    // data
    CCallsignList       m_NodeWhiteList;
    CCallsignList       m_NodeBlackList;
    CPeerCallsignList   m_PeerList;

    // thread
    std::atomic<bool>   m_bStopThread;
    std::thread         *m_pThread;

    // loop detection
    std::mutex          m_LoopMutex;
    CStreamHistory      m_StreamHistory[NB_OF_MODULES];
    std::map<CCallsign, CLoopPenalty>  m_LoopPenalties;
};


////////////////////////////////////////////////////////////////////////////////////////
#endif /* cgatekeeper_h */
