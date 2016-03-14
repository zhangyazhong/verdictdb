#include "confidenceInterval_udf.h"

#include <cctype>
#include <cmath>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include <algorithm>

StringVal confidenceInterval(FunctionContext* context, const FloatVal& conf, int num_var_args, const DoubleVal* args) {
	int c = num_var_args;
	double* nums = new double[c];
	for(int i=0;i<c;i++)
		nums[i]=args[i].val;
	std::sort(nums, nums+c);
    int margin = (int) (c * (1 - conf.val) / 2);
    double l = nums[margin], r = nums[c - margin - 1];
	char buf[50];
	sprintf(buf, "[%f, %f]", l, r);
	std::string str(buf);
	 StringVal result(context, str.size());
  memcpy(result.ptr, str.c_str(), str.size());
    return result;
}