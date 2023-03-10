//
// Created by user on 23-3-9.
//

#pragma once

#include "peer/replicator/block_fragment_generator.h"

namespace peer::v2 {
    class FragmentUtil {
    public:
        FragmentUtil(int localServerCount, int remoteServerCount) {
            reset(localServerCount, remoteServerCount);
        }

        void reset(int localServerCount, int remoteServerCount) {
            _localServerCount = localServerCount;
            _remoteServerCount = remoteServerCount;
            _totalFragments = LCM(_localServerCount, _remoteServerCount);
            _localFPS = _totalFragments/localServerCount;
            _remoteFPS = _totalFragments/remoteServerCount;
        }

        struct FragmentConfig {
            [[nodiscard]] bool operator==(const FragmentConfig &rhs) const {
                return startFragmentId == rhs.startFragmentId &&
                       endFragmentId == rhs.endFragmentId  &&
                       localId == rhs.localId &&
                       remoteId == rhs.remoteId;
            }

            int startFragmentId;
            int endFragmentId;
            int localId;
            int remoteId;
        };

        // How to allocate shards between two clusters
        // [start, end): the first int is the start fragment id, the next int is the end fragment id.
        // please use GenerateFragmentConfig to generate readable fragment
        [[nodiscard]] std::vector<FragmentConfig> getSenderConfig(int localId) const {
            const int start = _localFPS*localId;
            const int end = _localFPS*(localId+1);
            // Each server in region A is responsible for sending remoteServerCount fragments.
            // Each server in region B is responsible for sending localServerCount fragments.
            std::vector<FragmentConfig> retList;
            int nextStart=start;
            for (int i=start; i<end; i++) {
                if (i%_remoteFPS == _remoteFPS-1) {
                    // i+1 is the next fragment
                    FragmentConfig cfg{};
                    cfg.startFragmentId = nextStart;
                    cfg.endFragmentId = i+1;
                    cfg.localId = localId;
                    cfg.remoteId = (i + 1 - 1)/_remoteFPS;
                    retList.push_back(cfg);
                    nextStart = i+1;
                }
            }
            if (nextStart != end) {
                FragmentConfig cfg{};
                cfg.startFragmentId = nextStart;
                cfg.endFragmentId = end;
                cfg.localId = localId;
                cfg.remoteId = (end - 1)/_remoteFPS;
                retList.push_back(cfg);
            }
            return retList;
        }

        // The caller manually fills in concurrency and instanceCount (instanceCount is usually 1)
        [[nodiscard]] auto getBFGConfig() const {
            BlockFragmentGenerator::Config cfg;
            // If not divisible, take the remainder down
            cfg.parityShardCnt = static_cast<int>((2.0 * _totalFragments) / 3);
            cfg.dataShardCnt = _totalFragments - cfg.parityShardCnt;
            return cfg;
        }

        static int LCM(int n1, int n2) {
            int hcf = n1;
            int temp = n2;
            while(hcf != temp) {
                if(hcf > temp) {
                    hcf -= temp;
                } else {
                    temp -= hcf;
                }
            }
            return (n1 * n2) / hcf;
        }

    private:
        int _localServerCount{};
        int _remoteServerCount{};
        int _totalFragments{};    // totalFragments = LCM(localServerCount, remoteServerCount);
        int _localFPS{};          // localFPS = totalFragments/localServerCount;
        int _remoteFPS{};         // remoteFPS = totalFragments/remoteServerCount;
    };
}
