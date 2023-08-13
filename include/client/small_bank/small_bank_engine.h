//
// Created by user on 23-6-28.
//

#pragma once

#include "client/core/default_engine.h"
#include "client/small_bank/small_bank_workload.h"
#include "client/small_bank/small_bank_property.h"

namespace client::small_bank {
    struct SmallBankFactory {
        static std::shared_ptr<core::Workload> CreateWorkload(const util::Properties &) {
            return std::make_shared<SmallBankWorkload>();
        }

        static std::unique_ptr<SmallBankProperties> CreateProperty(const util::Properties &n) {
            return SmallBankProperties::NewFromProperty(n);
        }
    };

    using SmallBankEngine = core::DefaultEngine<SmallBankFactory, SmallBankProperties>;
}
