//
//  cp25protocol.h
//  xlxd
//
//  Created for P25 Reflector peering support
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

#ifndef cp25protocol_h
#define cp25protocol_h

#include <cstring>
#include <vector>
#include <mutex>
#include "ctimepoint.h"
#include "cprotocol.h"
#include "cdvheaderpacket.h"
#include "cdvframepacket.h"
#include "cdvlastframepacket.h"
#include "p25defines.h"
#include "cp25peer.h"
#include "cpeercallsignlist.h"

////////////////////////////////////////////////////////////////////////////////////////
// class

class CP25StreamCacheItem
{
public:
    CP25StreamCacheItem()      { m_uiSrcId = 0; m_uiDstTg = 0; m_uiFrameCount = 0; m_bPendingHeader = false; m_bHasOwner = false; }
    ~CP25StreamCacheItem()     { ClearPendingFrames(); }

    std::mutex      m_Mutex;        // protects fields from RX/TX thread races

    void ClearPendingFrames(void)
    {
        for (size_t i = 0; i < m_PendingFrames.size(); i++)
            delete m_PendingFrames[i];
        m_PendingFrames.clear();
        m_bPendingHeader = false;
    }

    CDvHeaderPacket m_dvHeader;
    uint32_t m_uiSrcId;
    uint32_t m_uiDstTg;
    unsigned int m_uiFrameCount;      // Counter for outgoing frame position (0-17)

    // Deferred header: buffer voice frames until source ID arrives
    bool m_bPendingHeader;
    uint32_t m_uiPendingStreamId;
    CIp m_PendingIp;
    std::vector<CDvFramePacket *> m_PendingFrames;
    CIp     m_OwnerIp;
    bool    m_bHasOwner;
};

class CP25Protocol : public CProtocol
{
public:
    // constructor
    CP25Protocol();

    // destructor
    virtual ~CP25Protocol() {};

    // initialization
    bool Init(void);
    void Close(void);

    // split-thread mode
    bool UsesSplitThreads(void) const { return true; }
    void RxTask(void);
    void TxTask(void);

protected:
    // queue helper
    void HandleQueue(void);

    // keepalive helpers
    void HandlePeerLinks(void);
    void HandlePeerKeepalives(void);

    // packet handlers
    void OnLduPacketIn(const CBuffer &, const CIp &, bool);
    void OnTerminatorPacketIn(const CIp &);

    // stream helpers
    bool OnDvHeaderPacketIn(CDvHeaderPacket *, const CIp &);

    // packet decoding helpers
    bool IsValidPollPacket(const CBuffer &, CCallsign *);
    bool ExtractImbeFromRecord(const CBuffer &, uint8 *);

    // packet encoding helpers
    void EncodePollPacket(CBuffer *, const CCallsign &) const;
    void EncodeVoicePacket(CBuffer *, unsigned int, const uint8 *, uint32_t, uint32_t) const;
    void EncodeTerminatorPacket(CBuffer *) const;

    // peer helpers
    bool IsP25PeerCallsign(const CCallsign &) const;
    CCallsignListItem *FindP25PeerByIp(CPeerCallsignList *, const CIp &);

    // P25 ID helpers
    uint32_t CallsignToP25Id(const CCallsign &) const;
    bool P25IdToCallsign(uint32_t p25Id, CCallsign *callsign) const;

    // uiStreamId helpers
    uint32 IpToStreamId(const CIp &) const;

protected:
    // for peer connections
    CTimePoint          m_LastPeerLinkTime;
    CTimePoint          m_LastPeerKeepaliveTime;

    // for queue header caches
    std::array<CP25StreamCacheItem, NB_OF_MODULES>    m_StreamsCache;
};

////////////////////////////////////////////////////////////////////////////////////////
#endif /* cp25protocol_h */
