#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <AggregateFunctions/SketchDataUtils.h>
#include <Core/Types.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Functions/DatasketchesIncludes.h>

namespace DB
{

template <typename T>
class KLLSketchData : private boost::noncopyable
{
private:
    std::unique_ptr<datasketches::kll_sketch<double>> kll_sketch;

    datasketches::kll_sketch<double> * getKLLSketch()
    {
        if (!kll_sketch)
            kll_sketch = std::make_unique<datasketches::kll_sketch<double>>(datasketches::kll_sketch<double>());
        return kll_sketch.get();
    }

public:
    KLLSketchData() = default;
    ~KLLSketchData() = default;

    void insertOriginal(double value)
    {
        getKLLSketch()->update(value);
    }

    void insertSerialized(std::string_view serialized_data, bool force_raw = true)
    {
        if (serialized_data.empty())
            return;

        std::string decoded_storage;
        /// When merging internally-generated sketches (from serializedKLL),
        /// we know the data is raw binary, not base64. Use force_raw=true for performance.
        /// For external data sources that might send base64, set force_raw=false.
        auto [data_ptr, data_size] = decodeSketchData(serialized_data, decoded_storage, force_raw);

        if (data_ptr == nullptr || data_size == 0)
            return;

        /// Deserialize and merge the sketch
        try
        {
            auto sk = datasketches::kll_sketch<double>::deserialize(data_ptr, data_size);
            getKLLSketch()->merge(sk);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// If deserialization fails (corrupted or invalid data), skip this value.
            /// This allows graceful handling of bad input data rather than failing the entire aggregation.
        }
    }

    String serializedData()
    {
        if (!kll_sketch)
        {
            return "";
        }
        auto bytes = kll_sketch->serialize();
        return String(bytes.begin(), bytes.end());
    }


    void merge(const KLLSketchData & rhs)
    {
        if (!rhs.kll_sketch)
            return;
        datasketches::kll_sketch<double> * u = getKLLSketch();
        u->merge(*const_cast<KLLSketchData &>(rhs).kll_sketch);
    }

    void read(DB::ReadBuffer & in)
    {
        std::vector<uint8_t> bytes;
        readVectorBinary(bytes, in);
        if (!bytes.empty())
        {
            auto kll_sketch_local = datasketches::kll_sketch<double>::deserialize(bytes.data(), bytes.size());
            getKLLSketch()->merge(kll_sketch_local);
        }
    }

    void write(DB::WriteBuffer & out) const
    {
        if (kll_sketch)
        {
            auto bytes = kll_sketch->serialize();
            writeVectorBinary(bytes, out);
        }
        else
        {
            std::vector<uint8_t> bytes;
            writeVectorBinary(bytes, out);
        }
    }
};

}

#endif
