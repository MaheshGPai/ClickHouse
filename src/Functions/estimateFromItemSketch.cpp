#include "config.h"

#if USE_DATASKETCHES

#include <AggregateFunctions/SketchDataUtils.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/DatasketchesIncludes.h>
#include <frequent_items_sketch.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionEstimateFromItemSketch : public IFunction
{
public:
    static constexpr auto name = "estimateFromItemSketch";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionEstimateFromItemSketch>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg_sketch = checkAndGetDataType<DataTypeString>(arguments[0].get());
        if (!arg_sketch)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, should be String",
                arguments[0]->getName(),
                getName());

        const auto * arg_item = checkAndGetDataType<DataTypeString>(arguments[1].get());
        if (!arg_item)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, should be String",
                arguments[1]->getName(),
                getName());

        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnUInt64::create();

        const auto * col_sketch = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_sketch)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal column {} of first argument of function {}, should be ColumnString",
                arguments[0].column->getName(),
                getName());

        const auto * col_item = checkAndGetColumn<ColumnString>(arguments[1].column.get());
        if (!col_item)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal column {} of second argument of function {}, should be ColumnString",
                arguments[1].column->getName(),
                getName());

        auto col_res = ColumnUInt64::create(input_rows_count);
        auto & vec_res = col_res->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view serialized_data = col_sketch->getDataAt(i);
            std::string_view item = col_item->getDataAt(i);

            if (serialized_data.empty())
            {
                vec_res[i] = 0;
                continue;
            }

            try
            {
                /// ClickHouse aggregate functions (serializedItemSketch, mergeSerializedItemSketch)
                /// always return raw binary data, never base64. Skip base64 detection for performance.
                /// If users need to decode base64 sketch data from external sources, they should
                /// use base64Decode() explicitly before calling this function.
                std::string decoded_storage;
                auto [data_ptr, data_size] = decodeSketchData(serialized_data, decoded_storage, /* force_raw= */ true);

                if (data_ptr == nullptr || data_size == 0)
                {
                    vec_res[i] = 0;
                    continue;
                }

                auto sketch = datasketches::frequent_items_sketch<std::string, UInt64>::deserialize(data_ptr, data_size);
                vec_res[i] = sketch.get_estimate(std::string(item));
            }
            catch (...)
            {
                vec_res[i] = 0;
            }
        }

        return col_res;
    }
};

}

REGISTER_FUNCTION(EstimateFromItemSketch)
{
    FunctionDocumentation::Description description = R"(
Extracts the estimated frequency (weight) of an item from a serialized Frequent Items (ItemsSketch) sketch.
If the input sketch is invalid or empty, returns 0.
)";
    FunctionDocumentation::Syntax syntax = "estimateFromItemSketch(serialized_item_sketch, item)";
    FunctionDocumentation::Arguments arguments = {
        {"serialized_item_sketch", "Serialized item sketch as a String.", {"String"}},
        {"item", "Item to query (String).", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the estimated frequency (weight) for the given item. Returns 0 if the sketch is empty, invalid, or the item is not tracked.",
        {"UInt64"},
    };
    FunctionDocumentation::Examples examples = {
        {
            "Basic usage",
            R"(
WITH s AS (SELECT serializedItemSketch(concat('k', toString(number % 3))) AS sk FROM numbers(30))
SELECT
    estimateFromItemSketch((SELECT sk FROM s), 'k0') AS k0,
    estimateFromItemSketch((SELECT sk FROM s), 'k1') AS k1,
    estimateFromItemSketch((SELECT sk FROM s), 'k2') AS k2;
            )",
            "",
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = FunctionDocumentation::VERSION_UNKNOWN;
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionEstimateFromItemSketch>(documentation);
}

}

#endif

