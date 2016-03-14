#ifndef CONF_INT_UDF_H
#define CONF_INT_UDF_H

#include <impala_udf/udf.h>

using namespace impala_udf;

StringVal confidenceInterval(FunctionContext* context, const FloatVal& conf, int num_var_args, const DoubleVal* args);

#endif