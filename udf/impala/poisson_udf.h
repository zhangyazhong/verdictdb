#ifndef POISSON_UDF_H
#define POISSON_UDF_H

#include <impala_udf/udf.h>

using namespace impala_udf;

TinyIntVal poisson(FunctionContext* context);

#endif