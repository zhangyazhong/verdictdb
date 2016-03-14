#include "poisson_udf.h"

#include <cctype>
#include <cmath>
#include <string>
#include <stdlib.h>

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

TinyIntVal poisson(FunctionContext* context) {
	double frac = (double) rand()/ RAND_MAX;
	if(frac<=probs[1])
	    return TinyIntVal(0);
	    else if(frac<=probs[2])
             	    return TinyIntVal(1);
             	     else if(frac<=probs[3])
                         	    return TinyIntVal(2);
                         else if(frac<=probs[4])
                                     	    return TinyIntVal(3);
                                     	    else if(frac<=probs[5])
                                                 	    return TinyIntVal(4);
                                                 	    else if(frac<=probs[6])
                                                             	    return TinyIntVal(5);
                                                             	    else if(frac<=probs[7])
                                                                         	    return TinyIntVal(6);
                                                                         	    else if(frac<=probs[8])
                                                                                     	    return TinyIntVal(7);
                                                                                     	    else if(frac<=probs[9])
                                                                                                 	    return TinyIntVal(8);
                                                                                                             	    else return TinyIntVal(9);
}