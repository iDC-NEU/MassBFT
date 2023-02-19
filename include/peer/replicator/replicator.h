//
// Created by peng on 2/18/23.
//

#ifndef NBP_REPLICATOR_H
#define NBP_REPLICATOR_H

#include "peer/replicator/mr_block_receiver.h"

namespace peer {
    class BFGWithThreadPool : public BlockFragmentGenerator {
    public:
        static std::shared_ptr<BFGWithThreadPool> NewBFGWithThreadPool(
                const std::vector<BlockFragmentGenerator::Config>& cfgList, int threadCount=0) {
            return std::shared_ptr<BFGWithThreadPool>(
                    new peer::BFGWithThreadPool(cfgList, std::make_unique<util::thread_pool_light>(threadCount)));
        }

        [[nodiscard]] const std::vector<Config>& getFragmentConfigList() const { return _fragmentConfigList; };

    protected:
        BFGWithThreadPool(std::vector<Config> cfgList, std::unique_ptr<util::thread_pool_light> tp)
                : BlockFragmentGenerator(cfgList, tp.get()), _fragmentConfigList(std::move(cfgList)), _tp(std::move(tp)) { }

    private:
        std::vector<Config> _fragmentConfigList;
        std::unique_ptr<util::thread_pool_light> _tp;
    };

}

#endif //NBP_REPLICATOR_H
