//
// Created by user on 23-8-4.
//
#include "common/proof_generator.h"

bool util::deserializeFromString(const std::string &raw, pmt::Proof &ret, std::vector<pmt::HashString> &container, int startPos)  {
    zpp::bits::in in(raw);
    in.reset(startPos);
    int64_t proofSize;
    if (failure(in(ret.Path, proofSize))) {
        return false;
    }
    container.resize(proofSize);
    ret.Siblings.resize(proofSize);
    for (int i=0; i<(int)proofSize; i++) {
        if(failure(in(container[i]))) {
            return false;
        }
        ret.Siblings[i] = &container[i];
    }
    return true;
}

bool util::serializeToString(const pmt::Proof &proof, std::string &ret, int startPos) {
    zpp::bits::out out(ret);
    out.reset(startPos);
    if (failure(out(proof.Path, (int64_t)proof.Siblings.size()))) {
        return false;
    }
    for (auto it : proof.Siblings) {
        if(failure(out(*it))) {
            return false;
        }
    }
    return true;
}