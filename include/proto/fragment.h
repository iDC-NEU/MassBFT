//
// Created by peng on 2/17/23.
//

#ifndef NBP_FRAGMENT_H
#define NBP_FRAGMENT_H

#include "common/parallel_merkle_tree.h"
#include "zpp_bits.h"

namespace proto {

    using BlockNumber = uint64_t;

    // EncodeMessage has type sv, so encoder / decoder must keep the actual message
    struct EncodeBlockFragment {
        constexpr static auto serialize(auto& archive, EncodeBlockFragment& self) {
            return archive(self.blockNumber, self.root, self.size, self.start, self.end, self.encodeMessage);
        }

        bool serializeWithoutMessage(std::string* rawEncodeMessage, int offset=0) {
            zpp::bits::out out(*rawEncodeMessage);
            out.reset(offset);
            if(failure(out(blockNumber, root, size, start, end))) {
                return false;
            }
            return true;
        }

        // block number must be equal to the actual block number
        BlockNumber blockNumber;
        // the size hint of the actual data, and root
        pmt::HashString root;
        size_t size;
        // the start and end fragment id
        uint32_t start;
        uint32_t end;
        // The local node does not need to sign the message,
        // because the point-to-point connection is secured by ssl
        std::string_view encodeMessage;
    };
}

#endif //NBP_FRAGMENT_H
