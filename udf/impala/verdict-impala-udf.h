#ifndef VERDICT_IMPALA_UDF_H
#define VERDICT_IMPALA_UDF_H

#include <impala_udf/udf.h>

using namespace impala_udf;

TinyIntVal Poisson(FunctionContext* context, const IntVal& index);

void CountInit(FunctionContext* context, BigIntVal* val);
void CountUpdate(FunctionContext* context, const IntVal& seed, const DoubleVal& input, BigIntVal* val);
void CountUpdate(FunctionContext* context, const IntVal& seed, const DoubleVal& input, const DoubleVal& weight, BigIntVal* val);
void CountMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst);
BigIntVal CountFinalize(FunctionContext* context, const BigIntVal& val);


void SumInit(FunctionContext* context, BigIntVal* val);
void SumUpdate(FunctionContext* context, const IntVal& seed, const IntVal& input, BigIntVal* val);
void SumMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst);
BigIntVal SumFinalize(FunctionContext* context, const BigIntVal& val);

void SumInit(FunctionContext* context, DoubleVal* val);
void SumUpdate(FunctionContext* context, const IntVal& seed, const DoubleVal& input, DoubleVal* val);
void SumMerge(FunctionContext* context, const DoubleVal& src, DoubleVal* dst);
DoubleVal SumFinalize(FunctionContext* context, const DoubleVal& val);

void AvgInit(FunctionContext* context, StringVal* val);
void AvgUpdate(FunctionContext* context, const IntVal& seed, const DoubleVal& input, StringVal* val);
void AvgUpdate(FunctionContext* context, const IntVal& seed, const DoubleVal& input, const DoubleVal& weight, StringVal* val);
void AvgMerge(FunctionContext* context, const StringVal& src, StringVal* dst);
DoubleVal AvgFinalize(FunctionContext* context, const StringVal& val);

#endif
