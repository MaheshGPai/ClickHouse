#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/ItemSketchData.h>
#include <Columns/ColumnString.h>

#if USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class AggregationFunctionMergeSerializedItemSketch final
    : public IAggregateFunctionDataHelper<ItemSketchData, AggregationFunctionMergeSerializedItemSketch>
{
private:
    bool base64_encoded; /// If true, data may be base64 encoded and needs decoding check

public:
    AggregationFunctionMergeSerializedItemSketch(const DataTypes & arguments, const Array & params, bool base64_encoded_)
        : IAggregateFunctionDataHelper<ItemSketchData, AggregationFunctionMergeSerializedItemSketch>{arguments, params, createResultType()}
        , base64_encoded(base64_encoded_)
    {}

    String getName() const override { return "mergeSerializedItemSketch"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeString>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColumnString &>(*columns[0]);
        auto serialized_data = column.getDataAt(row_num);
        this->data(place).insertSerialized(serialized_data, !base64_encoded);
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto serialized_data = this->data(place).serializedData();
        assert_cast<ColumnString &>(to).insertData(serialized_data.c_str(), serialized_data.size());
    }
};

AggregateFunctionPtr createAggregateFunctionMergeSerializedItemSketch(
    const String & name,
    const DataTypes & argument_types,
    const Array & params,
    const Settings *)
{
    /// Optional parameter: base64_encoded (default: false)
    bool base64_encoded = false;

    if (params.size() > 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} takes at most 1 parameter (base64_encoded: 0 or 1)",
            name);

    if (params.size() == 1)
        base64_encoded = params[0].safeGet<bool>();

    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Incorrect number of arguments for aggregate function {}", name);

    WhichDataType which(*argument_types[0]);
    if (!which.isStringOrFixedString())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function {}. Expected String or FixedString.",
            argument_types[0]->getName(),
            name);

    return std::make_shared<AggregationFunctionMergeSerializedItemSketch>(argument_types, params, base64_encoded);
}

}

/// mergeSerializedItemSketch - Merges multiple serialized Frequent Items (ItemsSketch) sketches into a single sketch.
///
/// Syntax: mergeSerializedItemSketch([base64_encoded])(sketch_column)
void registerAggregateFunctionMergeSerializedItemSketch(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };
    factory.registerFunction("mergeSerializedItemSketch", {createAggregateFunctionMergeSerializedItemSketch, properties});
}

}

#endif

