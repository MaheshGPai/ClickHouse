#include "config.h"

#if USE_DATASKETCHES

#include <AggregateFunctions/SketchDataUtils.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/DatasketchesIncludes.h>
#include <Interpreters/castColumn.h>
#include <frequent_items_sketch.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

class FunctionTopItemsFromItemSketch : public IFunction
{
public:
    static constexpr auto name = "topItemsFromItemSketch";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTopItemsFromItemSketch>(); }

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

        if (!isInteger(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, should be an integer",
                arguments[1]->getName(),
                getName());

        DataTypes tuple_elems{
            std::make_shared<DataTypeString>(),  // item
            std::make_shared<DataTypeUInt64>(),  // estimate
            std::make_shared<DataTypeUInt64>(),  // lower_bound
            std::make_shared<DataTypeUInt64>(),  // upper_bound
        };
        auto tuple_type = std::make_shared<DataTypeTuple>(tuple_elems);
        return std::make_shared<DataTypeArray>(tuple_type);
    }

    bool useDefaultImplementationForConstants() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnString * col_sketch = nullptr;
        bool is_sketch_const = false;
        std::string_view const_sketch_data;

        if (const auto * col_const_sketch = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
        {
            const_sketch_data = col_const_sketch->getDataAt(0);
            is_sketch_const = true;
        }
        else
        {
            col_sketch = checkAndGetColumn<ColumnString>(arguments[0].column.get());
            if (!col_sketch)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal column {} of first argument of function {}, should be ColumnString",
                    arguments[0].column->getName(),
                    getName());
        }

        /// Cast N to an integer column and validate.
        /// Query literals like `2` might be inferred as UInt8/Int8 depending on context.
        ColumnPtr n_column_casted;
        const ColumnUInt64 * col_n_u64 = nullptr;
        const ColumnInt64 * col_n_i64 = nullptr;
        bool is_n_const = false;
        UInt64 const_n_u64 = 0;
        Int64 const_n_i64 = 0;

        const WhichDataType which_n(*arguments[1].type);
        const bool n_is_signed = which_n.isInt();
        if (n_is_signed)
        {
            n_column_casted = castColumn(arguments[1], std::make_shared<DataTypeInt64>());
            if (const auto * col_const_n = checkAndGetColumnConst<ColumnInt64>(n_column_casted.get()))
            {
                const_n_i64 = col_const_n->getValue<Int64>();
                is_n_const = true;
            }
            else
            {
                col_n_i64 = checkAndGetColumn<ColumnInt64>(n_column_casted.get());
                if (!col_n_i64)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Failed to cast second argument of function {} to Int64", getName());
            }
        }
        else
        {
            n_column_casted = castColumn(arguments[1], std::make_shared<DataTypeUInt64>());
            if (const auto * col_const_n = checkAndGetColumnConst<ColumnUInt64>(n_column_casted.get()))
            {
                const_n_u64 = col_const_n->getValue<UInt64>();
                is_n_const = true;
            }
            else
            {
                col_n_u64 = checkAndGetColumn<ColumnUInt64>(n_column_casted.get());
                if (!col_n_u64)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Failed to cast second argument of function {} to UInt64", getName());
            }
        }

        const auto & array_type = assert_cast<const DataTypeArray &>(*result_type);
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*array_type.getNestedType());

        auto col_item = ColumnString::create();
        auto col_est = ColumnUInt64::create();
        auto col_lb = ColumnUInt64::create();
        auto col_ub = ColumnUInt64::create();
        auto offsets = ColumnVector<UInt64>::create();

        auto & item_chars = assert_cast<ColumnString &>(*col_item).getChars();
        auto & item_offsets = assert_cast<ColumnString &>(*col_item).getOffsets();
        auto & est = col_est->getData();
        auto & lb = col_lb->getData();
        auto & ub = col_ub->getData();
        auto & off = offsets->getData();

        using Sketch = datasketches::frequent_items_sketch<std::string, UInt64>;

        size_t total_elems = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view serialized_data = is_sketch_const ? const_sketch_data : col_sketch->getDataAt(i);
            UInt64 n = 0;
            if (n_is_signed)
            {
                const Int64 n_i64 = is_n_const ? const_n_i64 : col_n_i64->getInt(i);
                if (n_i64 <= 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "N must be > 0 for function {}", getName());
                n = static_cast<UInt64>(n_i64);
            }
            else
            {
                n = is_n_const ? const_n_u64 : col_n_u64->getUInt(i);
                if (n == 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "N must be > 0 for function {}", getName());
            }

            size_t row_added = 0;

            if (!serialized_data.empty())
            {
                try
                {
                    std::string decoded_storage;
                    auto [data_ptr, data_size] = decodeSketchData(serialized_data, decoded_storage, /* force_raw= */ true);
                    if (data_ptr && data_size)
                    {
                        auto sketch = Sketch::deserialize(data_ptr, data_size);
                        /// threshold=0 gives us all tracked items; result is sorted by estimate desc.
                        auto items = sketch.get_frequent_items(datasketches::NO_FALSE_NEGATIVES, static_cast<UInt64>(0));

                        const size_t limit = std::min<size_t>(items.size(), n);
                        for (size_t j = 0; j < limit; ++j)
                        {
                            const auto & r = items[j];
                            const std::string & s = r.get_item();

                            item_chars.insert(item_chars.end(), s.begin(), s.end());
                            item_offsets.push_back(item_chars.size());

                            est.push_back(r.get_estimate());
                            lb.push_back(r.get_lower_bound());
                            ub.push_back(r.get_upper_bound());
                        }

                        row_added = limit;
                    }
                }
                catch (...) // NOLINT(bugprone-empty-catch)
                {
                    /// Invalid sketch -> empty array.
                }
            }

            total_elems += row_added;
            off.push_back(total_elems);
        }

        std::vector<ColumnPtr> tuple_columns{
            std::move(col_item),
            std::move(col_est),
            std::move(col_lb),
            std::move(col_ub),
        };

        (void)tuple_type; // silence unused in some build configs
        auto tuple_col = ColumnTuple::create(std::move(tuple_columns));
        return ColumnArray::create(std::move(tuple_col), std::move(offsets));
    }
};

}

REGISTER_FUNCTION(TopItemsFromItemSketch)
{
    FunctionDocumentation::Description description = R"(
Returns the top N (by estimated frequency) items tracked by a serialized Frequent Items (ItemsSketch) sketch.
Each entry includes `(item, estimate, lower_bound, upper_bound)`.
If the sketch is empty/invalid, returns an empty array.
)";
    FunctionDocumentation::Syntax syntax = "topItemsFromItemSketch(serialized_item_sketch, N)";
    FunctionDocumentation::Arguments arguments = {
        {"serialized_item_sketch", "Serialized item sketch as a String.", {"String"}},
        {"N", "Number of items to return (UInt64, must be > 0).", {"UInt64"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns an array of tuples `(item, estimate, lower_bound, upper_bound)` sorted by `estimate` descending.",
        {"Array(Tuple(String, UInt64, UInt64, UInt64))"},
    };
    FunctionDocumentation::Examples examples = {
        {
            "Basic usage",
            R"(
WITH s AS (SELECT serializedItemSketch(concat('k', toString(number % 3))) AS sk FROM numbers(30))
SELECT topItemsFromItemSketch((SELECT sk FROM s), 2);
            )",
            "",
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = FunctionDocumentation::VERSION_UNKNOWN;
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTopItemsFromItemSketch>(documentation);
}

}

#endif

