#include "redismodule.h"
#include "command_filter.h"
#include "utils/dict.h"
#include "utils/buffer.h"
#include "utils/arr_rm_alloc.h"
#include "mgmt.h"
#include "commands.h"
#include "lock_handler.h"
#include <ctype.h>

#define COMMAND_FILTER_ARG_NAME "CommandFilterArg"
#define COMMAND_FILTER_ARG_VERSION 1

#define INNER_OVERIDE_COMMAND "rg.inneroveride"
#define INNER_OVERIDE_SLAVE_COMMAND "rg.inneroverideslave"

#define COMMAND_FILTER_DATATYPE_NAME "GEARS_CF0"
#define COMMAND_FILTER_DATATYPE_VERSION 1

static int CommandFilter_Serialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err);

typedef struct CallbackCtx{
    void* pd;
    char* callbackName;
    ArgType* type;
    RedisGears_CommandCallback callback;
}CallbackCtx;

typedef struct CommandInfoCtx{
    char* name;
    char* callback;
    void* pd;
    ArgType* type;
    RedisGears_CommandCallback c;
}CommandInfoCtx;

typedef struct CommandCtx{
    char* name;
    size_t index;
    CallbackCtx* callbacks;
}CommandCtx;

static RedisModuleCommandFilter* filterObj;
RedisModuleType *FilterDataType;
static Gears_dict* commandDict;
static ArgType* commandFilterArgType;

static CommandCtx* currCmdCtx;
static RedisModuleString* overideCmdName;

static void CommandFilter_Filter(RedisModuleCommandFilterCtx *filter){
    if(Gears_dictSize(commandDict) == 0){
        return;
    }
    const RedisModuleString *commandName = RedisModule_CommandFilterArgGet(filter, 0);
    size_t cmdNameLen;
    const char* commandNameCStr = RedisModule_StringPtrLen(commandName, &cmdNameLen);
    char commandNameToLower[cmdNameLen + 1];
    for(size_t i = 0 ; i < cmdNameLen ; ++i){
        commandNameToLower[i] = tolower(commandNameCStr[i]);
    }
    commandNameToLower[cmdNameLen] = '\0';
    CommandCtx* cmdCtx = Gears_dictFetchValue(commandDict, commandNameToLower);
    if(!cmdCtx){
        return;
    }
    if(cmdCtx->index < 0){
        return;
    }
    currCmdCtx = cmdCtx;
    RedisModule_CommandFilterArgInsert(filter, 0, overideCmdName);
}

static CommandCtx* CommandFilter_GetOrCreate(const char* name){
#define CALLBACKS_INIT_CAP 5
    CommandCtx* cmdCtx = Gears_dictFetchValue(commandDict, name);
    if(!cmdCtx){
        cmdCtx = RG_ALLOC(sizeof(*cmdCtx) + sizeof(CallbackCtx));
        cmdCtx->name = RG_STRDUP(name);
        cmdCtx->index = -1;
        cmdCtx->callbacks = array_new(CallbackCtx, CALLBACKS_INIT_CAP);
        Gears_dictAdd(commandDict, (char*)name, cmdCtx);
    }
    return cmdCtx;
}

static void CommandFilter_AddCallbackCtx(CommandCtx* cmdCtx, CommandInfoCtx* cmdInfoCtx){
    CallbackCtx callbackCtx = {
            .pd = cmdInfoCtx->type->dup(cmdInfoCtx->pd),
            .callbackName = RG_STRDUP(cmdInfoCtx->callback),
            .type = cmdInfoCtx->type,
            .callback = cmdInfoCtx->c,
    };
    cmdCtx->callbacks = array_append(cmdCtx->callbacks, callbackCtx);
    cmdCtx->index++;
}

static void CommandFilter_RegisterOnShard(ExecutionCtx* rctx, Record *data, void* arg){
    RedisModuleCtx* ctx = RedisGears_GetRedisModuleCtx(rctx);
    LockHandler_Acquire(ctx);
    CommandInfoCtx* cmdInfoCtx = arg;
    ArgType* type = CommandsMgmt_GetArgType(cmdInfoCtx->callback);
    RedisGears_CommandCallback c = CommandsMgmt_Get(cmdInfoCtx->callback);
    if(!type || !c){
        RedisGears_SetError(rctx, RG_STRDUP("Failed finding callback for command filter"));
        LockHandler_Release(ctx);
        return;
    }
    CommandCtx* cmdCtx = CommandFilter_GetOrCreate(cmdInfoCtx->name);
    CommandFilter_AddCallbackCtx(cmdCtx, cmdInfoCtx);

    // replicate to slave
    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);
    char* err = NULL;
    int res = CommandFilter_Serialize(NULL, cmdInfoCtx, &bw, &err);
    RedisModule_Assert(res == REDISMODULE_OK);
    RedisModule_Replicate(ctx, INNER_OVERIDE_SLAVE_COMMAND, "lb", COMMAND_FILTER_ARG_VERSION, buff->buff, buff->size);
    Gears_BufferFree(buff);

    LockHandler_Release(ctx);
}

static void CommandFilter_dropExecutionOnDone(ExecutionPlan* ep, void* privateData){
    RedisGears_DropExecution(ep);
}

static CommandInfoCtx* RedisGears_CommandCtxInfoCreate(const char* command, const char* callback, void* pd){
    ArgType* type = CommandsMgmt_GetArgType(callback);
    RedisGears_CommandCallback c = CommandsMgmt_Get(callback);
    if(!type || !c){
        return NULL;
    }
    CommandInfoCtx* cmdInfoCtx = RG_ALLOC(sizeof(*cmdInfoCtx));
    cmdInfoCtx->name = RG_STRDUP(command);
    cmdInfoCtx->callback = RG_STRDUP(callback);
    cmdInfoCtx->pd = pd;

    strtolower(cmdInfoCtx->name);

    cmdInfoCtx->type = type;
    cmdInfoCtx->c = c;

    return cmdInfoCtx;
}

int RG_CommandFilterAdd(const char* command, const char* callback, void* pd, char** err){
    CommandInfoCtx* cmdInfoCtx = RedisGears_CommandCtxInfoCreate(command, callback, pd);
    if(!cmdInfoCtx){
        *err = RG_STRDUP("Failed creating command info ctx");
        return REDISMODULE_ERR;
    }

    FlatExecutionPlan* fep = RGM_CreateCtx(ShardIDReader, err);
    if(!fep){
        *err = RG_STRDUP("Failed creating flat execution");
        return REDISMODULE_ERR;
    }
    // todo : make sure pd is serializable, save it as serializable
    // to make sure serialization and deserialization never fails.

    RGM_ForEach(fep, CommandFilter_RegisterOnShard, cmdInfoCtx);
    ExecutionPlan* ep = RedisGears_Run(fep, ExecutionModeAsync, NULL, CommandFilter_dropExecutionOnDone, NULL, mgmtWorker, err);
    RedisGears_FreeFlatExecution(fep);
    if(!ep){
        *err = RG_STRDUP("Failed running flat execution");
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

int RG_CommandFilterRegister(const char* callbackName, RedisGears_CommandCallback callback, ArgType* type){
    CommandsMgmt_Add(callbackName, callback, type);
    return REDISMODULE_OK;
}

static void CommandFilter_Free(void* arg){
    CommandInfoCtx* cmdInfoCtx = arg;
    ArgType* type = CommandsMgmt_GetArgType(cmdInfoCtx->callback);
    if(cmdInfoCtx->pd){
        type->free(cmdInfoCtx->pd);
    }
    RG_FREE(cmdInfoCtx->name);
    RG_FREE(cmdInfoCtx->callback);
    RG_FREE(cmdInfoCtx);
}

static void* CommandFilter_Duplicate(void* arg){
    CommandInfoCtx* cmdInfoCtx = arg;
    ArgType* type = CommandsMgmt_GetArgType(cmdInfoCtx->callback);
    CommandInfoCtx* dup = RG_ALLOC(sizeof(*dup));
    dup->name = RG_STRDUP(cmdInfoCtx->name);
    dup->callback = RG_STRDUP(cmdInfoCtx->callback);
    dup->pd = type->dup(cmdInfoCtx->pd);
    return dup;
}

static int CommandFilter_Serialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){
    CommandInfoCtx* cmdInfoCtx = arg;
    ArgType* type = CommandsMgmt_GetArgType(cmdInfoCtx->callback);
    RedisGears_BWWriteString(bw, cmdInfoCtx->name);
    RedisGears_BWWriteString(bw, cmdInfoCtx->callback);
    RedisGears_BWWriteLong(bw, type->version);
    return type->serialize(fep, cmdInfoCtx->pd, bw, err);
}

static void* CommandFilter_Deserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > COMMAND_FILTER_ARG_VERSION){
        *err = RG_STRDUP("Command filter version is higher then mine, please upgrade to the newer RedisGears module version");
        return NULL;
    }
    const char* name = RedisGears_BRReadString(br);
    const char* callback = RedisGears_BRReadString(br);
    long v = RedisGears_BRReadLong(br);
    CommandInfoCtx* cmdInfoCtx = RedisGears_CommandCtxInfoCreate(name, callback, NULL);
    cmdInfoCtx->pd = cmdInfoCtx->type->deserialize(fep, br, v, err);
    return cmdInfoCtx;
}

static char* CommandFilter_ToString(void* arg){
    return RG_STRDUP("CommandForEachCtx");
}

static int CommandFilter_OverideSlaveCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 3){
        return RedisModule_WrongArity(ctx);
    }
    long long version;
    if(RedisModule_StringToLongLong(argv[1], &version) != REDISMODULE_OK){
        RedisModule_ReplyWithError(ctx, "Failing parsing command filter version");
        return REDISMODULE_OK;
    }
    size_t serializedDataLen;
    const char* serializedData = RedisModule_StringPtrLen(argv[2], &serializedDataLen);
    Gears_Buffer buff;
    buff.buff = (char*)serializedData;
    buff.size = serializedDataLen;
    buff.cap = serializedDataLen;
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);
    char* err = NULL;
    CommandInfoCtx* cmdInfoCtx = CommandFilter_Deserialize(NULL, &br, version, &err);
    if(!cmdInfoCtx){
        if(!err){
            err = RG_STRDUP("Failed deserialing command filter info");
        }
        RedisModule_Log(ctx, "warning", "Faile deserializing command filter info, error='%s'", err);
        RedisModule_ReplyWithError(ctx, err);
        return REDISMODULE_OK;
    }

    CommandCtx* cmdCtx = CommandFilter_GetOrCreate(cmdInfoCtx->name);
    CommandFilter_AddCallbackCtx(cmdCtx, cmdInfoCtx);
    CommandFilter_Free(cmdInfoCtx);

    RedisModule_ReplicateVerbatim(ctx);

    return REDISMODULE_OK;
}

static int CommandFilter_OverideCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    CommandCtx* cmdCtx = currCmdCtx;
    CallbackCtx* callback = cmdCtx->callbacks + cmdCtx->index;
    --cmdCtx->index;
    callback->callback(ctx, argv + 1, argc - 1, callback->pd);
    ++cmdCtx->index;
    return REDISMODULE_OK;
}

static void CommandFilter_FreeCommandCtx(CommandCtx* cmdCtx){
    RG_FREE(cmdCtx->name);
    for(size_t i = 0 ; i < array_len(cmdCtx->callbacks) ; ++i){
        cmdCtx->callbacks[i].type->free(cmdCtx->callbacks[i].pd);
        RG_FREE(cmdCtx->callbacks[i].callbackName);
    }
    array_free(cmdCtx->callbacks);
    RG_FREE(cmdCtx);
}

static void CommandFilter_Clear(Gears_dict* d){
    if(!d){
        return;
    }
    Gears_dictIterator *iter = Gears_dictGetIterator(d);
    Gears_dictEntry* entry = NULL;
    while((entry = Gears_dictNext(iter))){
        CommandCtx* cmdCtx = Gears_dictGetVal(entry);
        CommandFilter_FreeCommandCtx(cmdCtx);
    }
    Gears_dictReleaseIterator(iter);
    Gears_dictRelease(d);
}

static int CommandFilter_AuxLoad(RedisModuleIO *rdb, int encver, int when){
    if(encver > COMMAND_FILTER_DATATYPE_VERSION){
        RedisModule_LogIOError(rdb, "warning", "Command filter data type version create by newer RedisGears module, please upgrade to the latest RedisGears version.");
        return REDISMODULE_ERR;
    }

    Gears_dict* tempDict = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);

    size_t len = RedisModule_LoadUnsigned(rdb);
    for(size_t i = 0 ; i < len ; ++i){
        // todo : extrac to function
        size_t nameLen;
        char* name = RedisModule_LoadStringBuffer(rdb, &nameLen);
        CommandCtx* cmdCtx = RG_ALLOC(sizeof(*cmdCtx) + sizeof(CallbackCtx));
        cmdCtx->name = RG_STRDUP(name);
        cmdCtx->index = -1;
        cmdCtx->callbacks = array_new(CallbackCtx, CALLBACKS_INIT_CAP);
        Gears_dictAdd(tempDict, name, cmdCtx);
        RedisModule_Free(name);

        size_t nCallbacks = RedisModule_LoadUnsigned(rdb);
        for(size_t i = 0 ; i < nCallbacks ; ++i){
            size_t callbackNameLen;
            char* callbackName = RedisModule_LoadStringBuffer(rdb, &callbackNameLen);
            size_t typeVersion = RedisModule_LoadUnsigned(rdb);
            size_t serializedPDLen;
            char* serializedPD = RedisModule_LoadStringBuffer(rdb, &serializedPDLen);

            CallbackCtx callbackCtx = {
                    .pd = NULL,
                    .callbackName = RG_STRDUP(callbackName),
                    .type = NULL,
                    .callback = NULL,
            };

            RedisModule_Free(callbackName);


            callbackCtx.type = CommandsMgmt_GetArgType(callbackName);
            callbackCtx.callback = CommandsMgmt_Get(callbackName);

            if(!callbackCtx.type || !callbackCtx.callback){
                RedisModule_LogIOError(rdb, "warning", "Failed finding callback for command filter, callback='%s'", callbackName);
                RedisModule_Free(serializedPD);
                RG_FREE(callbackCtx.callbackName);
                CommandFilter_Clear(tempDict);
                return REDISMODULE_ERR;
            }

            Gears_Buffer buff;
            buff.buff = serializedPD;
            buff.size = serializedPDLen;
            buff.cap = serializedPDLen;
            Gears_BufferReader br;
            Gears_BufferReaderInit(&br, &buff);
            char* err = NULL;
            callbackCtx.pd = callbackCtx.type->deserialize(NULL, &br, typeVersion, &err);
            RedisModule_Free(serializedPD);
            if(err){
                RedisModule_LogIOError(rdb, "warning", "Failed deserializing command filter PD, error='%s'", err);
                RG_FREE(err);
                CommandFilter_Clear(tempDict);
                return REDISMODULE_ERR;
            }

            cmdCtx->callbacks = array_append(cmdCtx->callbacks, callbackCtx);
            cmdCtx->index++;
        }
    }

    CommandFilter_Clear(commandDict);
    commandDict = tempDict;

    return REDISMODULE_OK;
}

static void CommandFilter_AuxSave(RedisModuleIO *rdb, int when){
    Gears_Buffer* buff = Gears_BufferCreate();
    RedisModule_SaveUnsigned(rdb, Gears_dictSize(commandDict));
    Gears_dictIterator *iter = Gears_dictGetIterator(commandDict);
    Gears_dictEntry* entry = NULL;
    while((entry = Gears_dictNext(iter))){
        CommandCtx* cmdCtx = Gears_dictGetVal(entry);
        RedisModule_SaveStringBuffer(rdb, cmdCtx->name, strlen(cmdCtx->name) + 1);
        RedisModule_SaveUnsigned(rdb, array_len(cmdCtx->callbacks));

        for(size_t i = 0 ; i < array_len(cmdCtx->callbacks) ; ++i){
            Gears_BufferClear(buff);
            CallbackCtx* callback = cmdCtx->callbacks + i;
            RedisModule_SaveStringBuffer(rdb, callback->callbackName, strlen(callback->callbackName) + 1);
            Gears_BufferWriter bw;
            Gears_BufferWriterInit(&bw, buff);
            char* err = NULL;
            RedisModule_SaveUnsigned(rdb, callback->type->version);
            callback->type->serialize(NULL, callback->pd, &bw, &err);
            if(err){
                RedisModule_LogIOError(rdb, "warning", "Failed serializing command filter PD, error='%s'", err);
                RedisModule_Assert(false);
            }
            RedisModule_SaveStringBuffer(rdb, buff->buff, buff->size);
        }
    }
    Gears_dictReleaseIterator(iter);
    Gears_BufferFree(buff);
}

static void CommandFilter_RegisterDataType(RedisModuleCtx* ctx){
    RedisModuleTypeMethods methods = {
                .version = REDISMODULE_TYPE_METHOD_VERSION,
                .rdb_load = NULL,
                .rdb_save = NULL,
                .aof_rewrite = NULL,
                .mem_usage = NULL,
                .digest = NULL,
                .free = NULL,
                .aux_load = CommandFilter_AuxLoad,
                .aux_save = CommandFilter_AuxSave,
                .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB,
            };

    RedisModuleType *FilterDataType = RedisModule_CreateDataType(ctx, COMMAND_FILTER_DATATYPE_NAME, COMMAND_FILTER_DATATYPE_VERSION, &methods);
}

int CommandFilter_CommandFilterInit(RedisModuleCtx* ctx){
    commandFilterArgType = RedisGears_CreateType("CommandFilterArg",
                                                  COMMAND_FILTER_ARG_VERSION,
                                                  CommandFilter_Free,
                                                  CommandFilter_Duplicate,
                                                  CommandFilter_Serialize,
                                                  CommandFilter_Deserialize,
                                                  CommandFilter_ToString);
    RGM_RegisterForEach(CommandFilter_RegisterOnShard, commandFilterArgType);

    filterObj = RedisModule_RegisterCommandFilter(ctx, CommandFilter_Filter, 0);

    if (RedisModule_CreateCommand(ctx, INNER_OVERIDE_COMMAND, CommandFilter_OverideCommand, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command "INNER_OVERIDE_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, INNER_OVERIDE_SLAVE_COMMAND, CommandFilter_OverideSlaveCommand, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command "INNER_OVERIDE_SLAVE_COMMAND);
        return REDISMODULE_ERR;
    }

    overideCmdName = RedisModule_CreateString(NULL, INNER_OVERIDE_COMMAND, strlen(INNER_OVERIDE_COMMAND));

    commandDict = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);

    CommandFilter_RegisterDataType(ctx);

    return REDISMODULE_OK;
}
