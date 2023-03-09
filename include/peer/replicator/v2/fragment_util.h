//
// Created by user on 23-3-9.
//

#pragma once

#include <vector>
#include "peer/replicator/block_fragment_generator.h"

namespace peer::v2 {
    class FragmentUtil {
    public:
        // How to allocate shards between two clusters
        static std::vector<std::pair<int, int>> CalculateFragmentConfig(int localServerCount, int remoteServerCount, int localId) {
            int totalFragments = LCM(localServerCount, remoteServerCount);
            auto localFPS = totalFragments/localServerCount;
            auto remoteFPS = totalFragments/remoteServerCount;
            int start = localFPS*localId;
            int end = localFPS*(localId+1);
            // Each server in region A is responsible for sending remoteServerCount fragments.
            // Each server in region B is responsible for sending localServerCount fragments.
            std::vector<std::pair<int, int>> retList;
            int left=start;
            bool leftover = false;
            for (int i=start; i<end; i++) {
                if (i%remoteFPS == remoteFPS-1) {
                    // i+1 is the next fragment
                    retList.emplace_back(left, i+1);
                    left = i+1;
                    leftover = false;
                    continue;
                }
                leftover = true;
            }
            if (leftover) {
                retList.emplace_back(left, end);
            }
            return retList;
        }

        // The caller manually fills in concurrency and instanceCount (instanceCount is usually 1)
        static auto GetBFGConfig(int localServerCount, int remoteServerCount) {
            BlockFragmentGenerator::Config cfg;
            int lcm = FragmentUtil::LCM(localServerCount, remoteServerCount);
            cfg.dataShardCnt = lcm / 3;     // If not divisible, take the remainder down
            cfg.parityShardCnt = lcm - cfg.dataShardCnt;
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
    };
}
