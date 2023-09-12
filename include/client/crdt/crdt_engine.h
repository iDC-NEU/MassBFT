//
// Created by user on 23-6-28.
//

#pragma once

#include "client/core/default_engine.h"
#include "client/crdt/crdt_workload.h"
#include "client/crdt/crdt_property.h"

namespace client::crdt {
    struct CrdtFactory {
        static std::shared_ptr<core::Workload> CreateWorkload(const util::Properties &) {
            return std::make_shared<CrdtWorkload>();
        }

        static std::unique_ptr<CrdtProperties> CreateProperty(const util::Properties &n) {
            return CrdtProperties::NewFromProperty(n);
        }
    };

    using CrdtEngine = core::DefaultEngine<CrdtFactory, CrdtProperties>;
}
