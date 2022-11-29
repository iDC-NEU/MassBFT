//
// Created by peng on 10/18/22.
//

#ifndef BENCHMARKCLIENT_MEASUREMENTS_H
#define BENCHMARKCLIENT_MEASUREMENTS_H

#include "ycsb/core/status.h"

#include <mutex>

namespace ycsb::core {
    /**
     * Collects latency measurements, and reports them when requested.
     */
    class Measurements {
    public:
        /**
         * All supported measurement types are defined in this enum.
         */
        enum class MeasurementType {
            HISTOGRAM,
            HDRHISTOGRAM,
            HDRHISTOGRAM_AND_HISTOGRAM,
            HDRHISTOGRAM_AND_RAW,
            TIMESERIES,
            RAW
        };

        constexpr static const auto MEASUREMENT_TYPE_PROPERTY = "measurementtype";
        constexpr static const auto MEASUREMENT_TYPE_PROPERTY_DEFAULT = "hdrhistogram";

        constexpr static const auto MEASUREMENT_INTERVAL = "measurement.interval";
        constexpr static const auto MEASUREMENT_INTERVAL_DEFAULT = "op";

        constexpr static const auto MEASUREMENT_TRACK_JVM_PROPERTY = "measurement.trackjvm";
        constexpr static const auto MEASUREMENT_TRACK_JVM_PROPERTY_DEFAULT = "false";

    private:
        static inline Measurements *singleton;
        static  inline std::mutex mutex;

    public:
        /**
         * Return the singleton Measurements object.
         */
        static Measurements *getMeasurements() {
            if (singleton == nullptr) {
                std::lock_guard lock(mutex);
                if (singleton == nullptr) {
                    singleton = new Measurements();
                }
            }
            return singleton;
        }

        explicit Measurements() {

        }
        void setIntendedStartTimeNs(long time) {

        }
        long getIntendedStartTimeNs() {
            return 0;
        }
        void measure(const std::string& operation, int latency) {

        }
        void measureIntended(const std::string& operation, int latency) {

        }
        void reportStatus(const std::string& operation, const Status& status) {

        }
    };

}
#endif //BENCHMARKCLIENT_MEASUREMENTS_H
