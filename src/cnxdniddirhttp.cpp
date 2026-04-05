//
//  cnxdniddirhttp.cpp
//  xlxd
//
//  Created for NXDN Reflector peering support
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

#include <string.h>
#include "main.h"
#include "creflector.h"
#include "cnxdniddirhttp.h"

#if (NXDNIDDB_USE_RLX_SERVER == 1)
CNxdnIdDirHttp g_NxdnIdDir;
#endif


////////////////////////////////////////////////////////////////////////////////////////
// refresh

bool CNxdnIdDirHttp::LoadContent(CBuffer *buffer)
{
    // Try primary source: radioid.net
    if ( HttpGet("database.radioid.net", "static/nxdn.csv", 80, buffer) )
    {
        m_LastSource = "radioid.net";
        return true;
    }

    // Fallback to pistar.uk mirror
    std::cout << "Primary NXDN database source failed, trying pistar.uk fallback..." << std::endl;
    if ( HttpGet("www.pistar.uk", "downloads/NXDN.csv", 80, buffer) )
    {
        m_LastSource = "pistar.uk";
        return true;
    }

    return false;
}

bool CNxdnIdDirHttp::RefreshContent(const CBuffer &buffer)
{
    bool ok = false;
    bool firstLine = true;

    // clear directory
    m_CallsignMap.clear();
    m_NxdnIdMap.clear();

    // scan file
    // CSV format: RADIO_ID,CALLSIGN,FIRST_NAME,LAST_NAME,CITY,STATE,COUNTRY
    if ( buffer.size() > 0 )
    {
        // Make a working copy since strtok modifies the string
        char *workbuf = new char[buffer.size() + 1];
        ::memcpy(workbuf, buffer.data(), buffer.size());
        workbuf[buffer.size()] = '\0';

        char *ptr1 = workbuf;
        char *ptr2;

        // Skip HTTP headers if present (find double newline)
        char *bodyStart = ::strstr(ptr1, "\r\n\r\n");
        if ( bodyStart != NULL )
        {
            ptr1 = bodyStart + 4;
        }

        // get next line
        while ( (ptr2 = ::strchr(ptr1, '\n')) != NULL )
        {
            *ptr2 = 0;

            // Remove carriage return if present
            if ( ptr2 > ptr1 && *(ptr2-1) == '\r' )
            {
                *(ptr2-1) = 0;
            }

            // Skip header line
            if ( firstLine )
            {
                firstLine = false;
                ptr1 = ptr2 + 1;
                continue;
            }

            // Parse CSV line: RADIO_ID,CALLSIGN,...
            // Make a copy of the line for parsing
            char linebuf[256];
            ::strncpy(linebuf, ptr1, sizeof(linebuf)-1);
            linebuf[sizeof(linebuf)-1] = '\0';

            char *nxdnid_str = ::strtok(linebuf, ",");
            char *callsign_str = ::strtok(NULL, ",");

            if ( nxdnid_str != NULL && callsign_str != NULL && IsValidNxdnId(nxdnid_str) )
            {
                uint16_t nxdnid = (uint16_t)::atoi(nxdnid_str);
                CCallsign cs(callsign_str);

                if ( cs.IsValid() && nxdnid > 0 )
                {
                    m_CallsignMap.insert(std::pair<uint16_t,CCallsign>(nxdnid, cs));
                    m_NxdnIdMap.insert(std::pair<CCallsign,uint16_t>(cs, nxdnid));
                }
            }

            // next line
            ptr1 = ptr2 + 1;
        }

        delete[] workbuf;

        // done
        ok = true;
    }

    // report
    std::cout << "Read " << m_NxdnIdMap.size() << " NXDN ids from " << m_LastSource << " database " << std::endl;

    // done
    return ok;
}


////////////////////////////////////////////////////////////////////////////////////////
// httpd helpers

#define NXDNID_HTTPGET_SIZEMAX       (512)

bool CNxdnIdDirHttp::HttpGet(const char *hostname, const char *filename, int port, CBuffer *buffer)
{
    bool ok = false;
    int sock_id;

    // open socket
    if ( (sock_id = ::socket(AF_INET, SOCK_STREAM, 0)) >= 0 )
    {
        // get hostname address
        struct sockaddr_in servaddr;
        struct hostent *hp;
        ::memset(&servaddr,0,sizeof(servaddr));
        if( (hp = gethostbyname(hostname)) != NULL )
        {
            // dns resolved
            ::memcpy((char *)&servaddr.sin_addr.s_addr, (char *)hp->h_addr, hp->h_length);
            servaddr.sin_port = htons(port);
            servaddr.sin_family = AF_INET;

            // connect
            if ( ::connect(sock_id, (struct sockaddr *)&servaddr, sizeof(servaddr)) == 0)
            {
                // send the GET request with proper user-agent
                char request[NXDNID_HTTPGET_SIZEMAX];
                ::snprintf(request, sizeof(request),
                          "GET /%s HTTP/1.0\r\nHost: %s\r\nFrom: %s\r\nUser-Agent: XLX %d.%d.%d NXDN Client Updater\r\n\r\n",
                          filename, hostname, (const char *)g_Reflector.GetCallsign(),
                          VERSION_MAJOR, VERSION_MINOR, VERSION_REVISION);
                ::write(sock_id, request, strlen(request));

                // config receive timeouts
                fd_set read_set;
                struct timeval timeout;
                timeout.tv_sec = 10;
                timeout.tv_usec = 0;
                FD_ZERO(&read_set);
                FD_SET(sock_id, &read_set);

                // get the reply back
                buffer->clear();
                bool done = false;
                do
                {
                    char buf[1440];
                    ssize_t len = 0;
                    select(sock_id+1, &read_set, NULL, NULL, &timeout);
                    usleep(5000);
                    len = read(sock_id, buf, 1440);
                    if ( len > 0 )
                    {
                        buffer->Append((uint8 *)buf, (int)len);
                        ok = true;
                    }
                    done = (len <= 0);

                } while (!done);
                buffer->Append((uint8)0);

                // and disconnect
                close(sock_id);
            }
            else
            {
                std::cout << "Cannot establish connection with host " << hostname << std::endl;
            }
        }
        else
        {
            std::cout << "Host " << hostname << " not found" << std::endl;
        }

    }
    else
    {
        std::cout << "Failed to open wget socket" << std::endl;
    }

    // done
    return ok;
}
