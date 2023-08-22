//
// Created by peng on 2/17/23.
//

#ifndef NBP_FRAGMENT_H
#define NBP_FRAGMENT_H

#include "common/parallel_merkle_tree.h"
#include "proto/block.h"
#include "zpp_bits.h"

namespace proto {

    using BlockNumber = uint64_t;

    // Do not serialize this class using in() or out() method
    // EncodeMessage has type sv, so encoder / decoder must keep the actual message
    struct EncodeBlockFragment {
        bool serializeToString(std::string* rawEncodeMessage, int offset, bool withBody) {
            zpp::bits::out out(*rawEncodeMessage);
            out.reset(offset);
            if(failure(out(blockSignatures, blockNumber, root, size, start, end))) {
                return false;
            }
            if (!withBody) {
                return true;
            }
            rawEncodeMessage->resize(out.position()+encodeMessage.size());
            std::memcpy(rawEncodeMessage->data()+out.position(), encodeMessage.data(), encodeMessage.size());
            return true;
        }

        bool deserializeFromString(std::string_view raw, int offset=0) {
            zpp::bits::in in(raw);
            in.reset(offset);
            if(failure(in(blockSignatures, blockNumber, root, size, start, end))) {
                return false;
            }
            // encodeMessage may be larger than expected
            encodeMessage = raw.substr(in.position());
            return true;
        }
        // block consensus signatures
        std::vector<proto::Block::SignaturePair> blockSignatures;

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
