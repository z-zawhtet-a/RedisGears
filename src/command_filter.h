#ifndef SRC_COMMAND_FILTER_H_
#define SRC_COMMAND_FILTER_H_

#include "redisgears.h"

int RG_CommandFilterAdd(const char* command, const char* callback, void* pd, char** err);

int RG_CommandFilterRegister(const char* callbackName, RedisGears_CommandCallback callback, ArgType* type);

int CommandFilter_CommandFilterInit(RedisModuleCtx* ctx);


#endif /* SRC_COMMAND_FILTER_H_ */
