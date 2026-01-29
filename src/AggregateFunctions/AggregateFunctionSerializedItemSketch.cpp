#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Common/FieldVisitorConvertToNumber.h>
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

class AggregationFunctionSerializedItemSketch final
    : public IAggregateFunctionDataHelper<ItemSketchData, AggregationFunctionSerializedItemSketch>
{
private:
    uint8_t lg_max_map_size;

public:
    AggregationFunctionSerializedItemSketch(const DataTypes & arguments, const Array & params, uint8_t lg_max_map_size_)
        : IAggregateFunctionDataHelper<ItemSketchData, AggregationFunctionSerializedItemSketch>{arguments, params, createResultType()}
        , lg_max_map_size(lg_max_map_size_)
    {}

    String getName() const override { return "serializedItemSketch"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeString>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override // NOLINT(readability-non-const-parameter)
    {
        new (place) ItemSketchData(lg_max_map_size);
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & item_col = *columns[0];
        auto item = item_col.getDataAt(row_num);

        UInt64 weight = 1;
        if (this->argument_types.size() == 2)
        {
            const auto & weight_col = *columns[1];
            weight = weight_col.getUInt(row_num);
        }

        this->data(place).insertOriginal(item, weight);
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

AggregateFunctionPtr createAggregateFunctionSerializedItemSketch(
    const String & name,
    const DataTypes & argument_types,
    const Array & params,
    const Settings *)
{
    /// Optional parameter: lg_max_map_size (default: DEFAULT_LG_MAX_MAP_SIZE)
    uint8_t lg_max_map_size = DEFAULT_LG_MAX_MAP_SIZE;

    if (params.size() > 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} takes at most 1 parameter: lg_max_map_size (>= {})",
            name,
            int(FrequentItemsSketch::LG_MIN_MAP_SIZE));

    if (!params.empty())
    {
        UInt64 lg_max_map_size_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
        if (lg_max_map_size_param < FrequentItemsSketch::LG_MIN_MAP_SIZE)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Parameter lg_max_map_size for aggregate function {} must be >= {}, got {}",
                name,
                int(FrequentItemsSketch::LG_MIN_MAP_SIZE),
                lg_max_map_size_param);
        lg_max_map_size = static_cast<uint8_t>(lg_max_map_size_param);
    }

    if (argument_types.size() != 1 && argument_types.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments for aggregate function {}. Expected 1 (item) or 2 (item, weight).",
            name);

    WhichDataType which_item(*argument_types[0]);
    if (!which_item.isStringOrFixedString())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of first argument for aggregate function {}. Expected String or FixedString.",
            argument_types[0]->getName(),
            name);

    if (argument_types.size() == 2)
    {
        WhichDataType which_weight(*argument_types[1]);
        if (!which_weight.isUInt64() && !which_weight.isUInt32() && !which_weight.isUInt16() && !which_weight.isUInt8()
            && !which_weight.isInt64() && !which_weight.isInt32() && !which_weight.isInt16() && !which_weight.isInt8())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument for aggregate function {}. Expected integer weight.",
                argument_types[1]->getName(),
                name);
    }

    return std::make_shared<AggregationFunctionSerializedItemSketch>(argument_types, params, lg_max_map_size);
}

}

/// serializedItemSketch - Creates a serialized Frequent Items (ItemsSketch) for approximate frequency estimation.
///
/// The sketch can be stored, transmitted, merged with mergeSerializedItemSketch(), and queried with estimateFromItemSketch().
///
/// Syntax:
///   serializedItemSketch([lg_max_map_size])(item [, weight])
///
/// Arguments:
///   - item: String
///   - weight (optional): integer (frequency increment)
///
/// Returns:
///   - String: serialized binary sketch
void registerAggregateFunctionSerializedItemSketch(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };
    factory.registerFunction("serializedItemSketch", {createAggregateFunctionSerializedItemSketch, properties});
}

}

#endif

