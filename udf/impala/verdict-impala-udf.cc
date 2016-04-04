#include "verdict-impala-udf.h"

#include <cctype>
#include <cmath>
#include <string>
#include <stdlib.h>

//TODO: increase to 20
double probs[10] = {0
 , 0.3678794469446667
 , 0.7357588622887303
 , 0.9196985778609128
 , 0.981011816384974
 , 0.9963401260159892
 , 0.999405787942192
 , 0.9999167718585252
 , 0.9999897235471829
 , 0.9999988878062837};

inline char poisson() {
	double frac = (double) rand()/ RAND_MAX;
	if(frac<=probs[1])
	    return (char)0;
    else if(frac<=probs[2])
        return (char)1;
    else if(frac<=probs[3])
        return (char)2;
    else if(frac<=probs[4])
        return (char)3;
    else if(frac<=probs[5])
        return (char)4;
    else if(frac<=probs[6])
        return (char)5;
    else if(frac<=probs[7])
        return (char)6;
    else if(frac<=probs[8])
        return (char)7;
    else if(frac<=probs[9])
        return (char)8;
    else return (char)9;
}

/*
POISSON(index)
*/

TinyIntVal Poisson(FunctionContext* context, const IntVal& index) {
    return TinyIntVal(poisson());
}


/*
COUNT(seed)
*/

void CountInit(FunctionContext* context, BigIntVal* val) {
  val->is_null = false;
  val->val = 0;
}

void CountUpdate(FunctionContext* context, const IntVal& seed, BigIntVal* val) {
  val->val += poisson();
}

void CountMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  dst->val += src.val;
}

BigIntVal CountFinalize(FunctionContext* context, const BigIntVal& val) {
  return val;
}


/*
SUM(seed, INT val)
*/

void SumInit(FunctionContext* context, BigIntVal* val) {
  val->is_null = false;
  val->val = 0;
}

void SumUpdate(FunctionContext* context, const IntVal& seed, const IntVal& input, BigIntVal* val) {
  if (input.is_null) return;
  val->val += input.val * poisson();
}

void SumMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  dst->val += src.val;
}

BigIntVal SumFinalize(FunctionContext* context, const BigIntVal& val) {
  return val;
}


/*
SUM(seed, DOUBLE val)
*/

void SumInit(FunctionContext* context, DoubleVal* val) {
  val->is_null = false;
  val->val = 0;
}

void SumUpdate(FunctionContext* context, const IntVal& seed, const DoubleVal& input, DoubleVal* val) {
  if (input.is_null) return;
  val->val += input.val * poisson();
}

void SumMerge(FunctionContext* context, const DoubleVal& src, DoubleVal* dst) {
  dst->val += src.val;
}

DoubleVal SumFinalize(FunctionContext* context, const DoubleVal& val) {
  return val;
}


/*
AVG(seed, val)
*/

struct AvgStruct {
  double sum;
  int64_t count;
};

void AvgInit(FunctionContext* context, StringVal* val) {
  // assert(sizeof(AvgStruct) == 16);
  val->len = sizeof(AvgStruct);
  val->ptr = context->Allocate(val->len);
  memset(val->ptr, 0, val->len);
}

void AvgUpdate(FunctionContext* context,const IntVal& seed, const DoubleVal& input, StringVal* val) {
if (input.is_null) return;
   AvgStruct* avg = reinterpret_cast<AvgStruct*>(val->ptr);
	char p = poisson();
  avg->sum += input.val * p;
  avg->count += p;
}

void AvgMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  const AvgStruct* src_struct = reinterpret_cast<const AvgStruct*>(src.ptr);
  AvgStruct* dst_struct = reinterpret_cast<AvgStruct*>(dst->ptr);
  dst_struct->sum += src_struct->sum;
  dst_struct->count += src_struct->count;
}

DoubleVal AvgFinalize(FunctionContext* context, const StringVal& val) {
  AvgStruct* val_struct = reinterpret_cast<AvgStruct*>(val.ptr);
  if (val_struct->count == 0)
    return DoubleVal::null();
  return DoubleVal(val_struct->sum / val_struct->count);
}