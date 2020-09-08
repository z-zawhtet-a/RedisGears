#include "redismodule.h"
#include "command_filter.h"
#include "utils/dict.h"
#include "utils/buffer.h"
#include "utils/arr_rm_alloc.h"
#include "mgmt.h"
#include "commands.h"
#include "lock_handler.h"
#include <ctype.h>

#define REGISTER_COMMAND_ARG_VERSION 1
#define UNREGISTER_COMMAND_ARG_VERSION 1

#define INNER_OVERIDE_COMMAND "rg.execute"
#define INNER_OVERIDE_SLAVE_COMMAND "rg.inneroverideslave"
#define INNER_REGISTER_COMMAND_FILTER "rg.innerregistercommandfilter"
#define INNER_UNREGISTER_COMMAND_FILTER "rg.innerunregistercommandfilter"

static bool invokedInternaly = false;

#define COMMAND_FILTER_DATATYPE_NAME "GEARS_CF0"
#define COMMAND_FILTER_DATATYPE_VERSION 1

typedef struct CallbackCtx CallbackCtx;
typedef struct CommandCtx CommandCtx;

static int CommandFilter_RegisterArgSerialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err);
static void CommandFilter_FreeCommandCtx(CommandCtx* cmdCtx);
static void CommandFilter_FreeCallbackCtx(CallbackCtx* callbackCtx);
static void CommandFilter_RegisterArgFree(void* arg);

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

typedef struct CommandInfoUnregisterCtx{
    char* name;
    size_t index;
}CommandInfoUnregisterCtx;

typedef struct CommandCtx{
    char* name;
    int index;
    CallbackCtx* callbacks;
}CommandCtx;

static RedisModuleCommandFilter* filterObj = NULL;
static RedisModuleType *FilterDataType = NULL;
static Gears_dict* commandDict = NULL;
static ArgType* registerCommandArgType = NULL;
static ArgType* unregisterCommandArgType = NULL;

static CommandCtx* currCmdCtx;
static RedisModuleString* overideCmdName;

static void CommandFilter_RegisterFilterIfNoRegitered(){
    if(!filterObj){
        RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
        invokedInternaly = true;
        RedisModuleCallReply *rep = RedisModule_Call(ctx, INNER_REGISTER_COMMAND_FILTER, "");
        invokedInternaly = false;
        RedisModule_FreeCallReply(rep);
        RedisModule_FreeThreadSafeContext(ctx);
    }
}

static void CommandFilter_UnregisterFilter(){
    if(filterObj){
        RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
        invokedInternaly = true;
        RedisModuleCallReply *rep = RedisModule_Call(ctx, INNER_UNREGISTER_COMMAND_FILTER, "");
        invokedInternaly = false;
        RedisModule_FreeCallReply(rep);
        RedisModule_FreeThreadSafeContext(ctx);
    }
}

static void CommandFilter_Filter(RedisModuleCommandFilterCtx *filter){
    if(!commandDict){
        return;
    }
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
    RedisModule_RetainString(NULL, overideCmdName);
    RedisModule_CommandFilterArgInsert(filter, 0, overideCmdName);
}

static CommandCtx* CommandFilter_GetOrCreate(const char* name){
#define CALLBACKS_INIT_CAP 5
    CommandFilter_RegisterFilterIfNoRegitered();
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

static void CommandFilter_UnregisterOnShard(ExecutionCtx* rctx, Record *data, void* arg){
    RedisModuleCtx* ctx = RedisGears_GetRedisModuleCtx(rctx);
    LockHandler_Acquire(ctx);

    CommandInfoUnregisterCtx* cmdInfoUnregisterCtx = arg;

    CommandCtx* cmdCtx = Gears_dictFetchValue(commandDict, cmdInfoUnregisterCtx->name);
    if(!cmdCtx){
        RedisGears_SetError(rctx, RG_STRDUP("Give command does not exists"));
        LockHandler_Release(ctx);
        return;
    }

    if(cmdInfoUnregisterCtx->index != -1){
        if(cmdInfoUnregisterCtx->index < 0 || cmdInfoUnregisterCtx->index >= array_len(cmdCtx->callbacks)){
            RedisGears_SetError(rctx, RG_STRDUP("Give index is out of range"));
            LockHandler_Release(ctx);
            return;
        }

        CommandFilter_FreeCallbackCtx(cmdCtx->callbacks + cmdInfoUnregisterCtx->index);
        array_del(cmdCtx->callbacks, cmdInfoUnregisterCtx->index);
        --cmdCtx->index;
    }

    if(array_len(cmdCtx->callbacks) == 0 || cmdInfoUnregisterCtx->index == -1){
        Gears_dictDelete(commandDict, cmdInfoUnregisterCtx->name);
        CommandFilter_FreeCommandCtx(cmdCtx);
        if(Gears_dictSize(commandDict) == 0){
            CommandFilter_UnregisterFilter();
        }
    }

    LockHandler_Release(ctx);
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
    int res = CommandFilter_RegisterArgSerialize(NULL, cmdInfoCtx, &bw, &err);
    RedisModule_Assert(res == REDISMODULE_OK);
    RedisModule_Replicate(ctx, INNER_OVERIDE_SLAVE_COMMAND, "lb", REGISTER_COMMAND_ARG_VERSION, buff->buff, buff->size);
    Gears_BufferFree(buff);

    LockHandler_Release(ctx);
}

static void CommandFilter_dropExecutionOnDone(ExecutionPlan* ep, void* privateData){
    RedisGears_DropExecution(ep);
}

static void CommandFilter_SendReply(ExecutionPlan* ep, void* privateData){
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(bc);
    size_t nerrors = RedisGears_GetErrorsLen(ep);
    if(nerrors){
        Record* error = RedisGears_GetError(ep, 0);
        const char* errorStr = RedisGears_StringRecordGet(error, NULL);
        RedisModule_ReplyWithError(ctx, errorStr);
        return;
    }else{
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

    RedisModule_UnblockClient(bc, NULL);
    RedisModule_FreeThreadSafeContext(ctx);
    return;
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
    cmdInfoCtx->pd = NULL;
    if(pd){
        cmdInfoCtx->pd = type->dup(pd);
    }

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

    if(strlen(cmdInfoCtx->name) >=3 && cmdInfoCtx->name[0] == 'r' && cmdInfoCtx->name[1] == 'g' && cmdInfoCtx->name[2] == '.'){
        CommandFilter_RegisterArgFree(cmdInfoCtx);
        *err = RG_STRDUP("Can not overide RedisGears commands");
        return REDISMODULE_ERR;
    }

    FlatExecutionPlan* fep = RGM_CreateCtx(ShardIDReader, err);
    if(!fep){
        CommandFilter_RegisterArgFree(cmdInfoCtx);
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

static void CommandFilter_UnregisterArgFree(void* arg){
    CommandInfoUnregisterCtx* cmdInfoUnregisterCtx = arg;
    RG_FREE(cmdInfoUnregisterCtx->name);
    RG_FREE(cmdInfoUnregisterCtx);
}

static void CommandFilter_RegisterArgFree(void* arg){
    CommandInfoCtx* cmdInfoCtx = arg;
    ArgType* type = CommandsMgmt_GetArgType(cmdInfoCtx->callback);
    if(cmdInfoCtx->pd){
        type->free(cmdInfoCtx->pd);
    }
    RG_FREE(cmdInfoCtx->name);
    RG_FREE(cmdInfoCtx->callback);
    RG_FREE(cmdInfoCtx);
}

static void* CommandFilter_UnregisterArgDuplicate(void* arg){
    CommandInfoUnregisterCtx* cmdInfoUnregisterCtx = arg;
    CommandInfoUnregisterCtx* dup = RG_ALLOC(sizeof(*dup));
    dup->name = RG_STRDUP(cmdInfoUnregisterCtx->name);
    dup->index = cmdInfoUnregisterCtx->index;
    return dup;
}

static void* CommandFilter_RegisterArgDuplicate(void* arg){
    CommandInfoCtx* cmdInfoCtx = arg;
    ArgType* type = CommandsMgmt_GetArgType(cmdInfoCtx->callback);
    CommandInfoCtx* dup = RG_ALLOC(sizeof(*dup));
    dup->name = RG_STRDUP(cmdInfoCtx->name);
    dup->callback = RG_STRDUP(cmdInfoCtx->callback);
    dup->pd = type->dup(cmdInfoCtx->pd);
    return dup;
}

static int CommandFilter_RegisterArgSerialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){
    CommandInfoCtx* cmdInfoCtx = arg;
    ArgType* type = CommandsMgmt_GetArgType(cmdInfoCtx->callback);
    RedisGears_BWWriteString(bw, cmdInfoCtx->name);
    RedisGears_BWWriteString(bw, cmdInfoCtx->callback);
    RedisGears_BWWriteLong(bw, type->version);
    return type->serialize(fep, cmdInfoCtx->pd, bw, err);
}

static int CommandFilter_UnregisterArgSerialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){
    CommandInfoUnregisterCtx* cmdInfoUnregisterCtx = arg;
    RedisGears_BWWriteString(bw, cmdInfoUnregisterCtx->name);
    RedisGears_BWWriteLong(bw, cmdInfoUnregisterCtx->index);
    return REDISMODULE_OK;
}

static void* CommandFilter_UnregisterArgDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > UNREGISTER_COMMAND_ARG_VERSION){
        *err = RG_STRDUP("Command unregister version is higher than mine, please upgrade to the newer RedisGears module version");
        return NULL;
    }

    CommandInfoUnregisterCtx* res = RG_ALLOC(sizeof(*res));
    res->name = RG_STRDUP(RedisGears_BRReadString(br));
    res->index = RedisGears_BRReadLong(br);
    return res;
}

static void* CommandFilter_RegisterArgDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > REGISTER_COMMAND_ARG_VERSION){
        *err = RG_STRDUP("Command register version is higher than mine, please upgrade to the newer RedisGears module version");
        return NULL;
    }
    const char* name = RedisGears_BRReadString(br);
    const char* callback = RedisGears_BRReadString(br);
    long v = RedisGears_BRReadLong(br);
    CommandInfoCtx* cmdInfoCtx = RedisGears_CommandCtxInfoCreate(name, callback, NULL);
    cmdInfoCtx->pd = cmdInfoCtx->type->deserialize(fep, br, v, err);
    return cmdInfoCtx;
}

static char* CommandFilter_RegisterArgToString(void* arg){
    return RG_STRDUP("RegisterCmdCtx");
}

static char* CommandFilter_UnregisterArgToString(void* arg){
    return RG_STRDUP("UnregisterCmdCtx");
}

static int CommandFilter_UnregisterFilterInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(!invokedInternaly){
        RedisModule_ReplyWithError(ctx, "This is an internal command and should not be invoked by user.");
        return REDISMODULE_OK;
    }
    if(filterObj){
        RedisModule_UnregisterCommandFilter(ctx, filterObj);
        filterObj = NULL;
    }
    RedisModule_ReplyWithCString(ctx, "OK");
    return REDISMODULE_OK;
}

static int CommandFilter_RegisterFilterInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(!invokedInternaly){
        RedisModule_ReplyWithError(ctx, "This is an internal command and should not be invoked by user.");
        return REDISMODULE_OK;
    }
    if(!filterObj){
        filterObj = RedisModule_RegisterCommandFilter(ctx, CommandFilter_Filter, 0);
    }
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
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
    CommandInfoCtx* cmdInfoCtx = CommandFilter_RegisterArgDeserialize(NULL, &br, version, &err);
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
    CommandFilter_RegisterArgFree(cmdInfoCtx);

    RedisModule_ReplicateVerbatim(ctx);

    return REDISMODULE_OK;
}

static int CommandFilter_OverideCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    CommandCtx* cmdCtx = currCmdCtx;
    currCmdCtx = NULL;
    if(!cmdCtx){
        if(argc < 2){
            return RedisModule_WrongArity(ctx);
        }
        RedisModuleString *commandName = argv[1];
        size_t cmdNameLen;
        const char* commandNameCStr = RedisModule_StringPtrLen(commandName, &cmdNameLen);
        char commandNameToLower[cmdNameLen + 1];
        for(size_t i = 0 ; i < cmdNameLen ; ++i){
            commandNameToLower[i] = tolower(commandNameCStr[i]);
        }
        commandNameToLower[cmdNameLen] = '\0';
        cmdCtx = Gears_dictFetchValue(commandDict, commandNameToLower);
        if(!cmdCtx){
            RedisModule_ReplyWithError(ctx, "Unknown command");
            return REDISMODULE_OK;
        }
    }
    CallbackCtx* callback = cmdCtx->callbacks + cmdCtx->index;
    --cmdCtx->index;
    callback->callback(ctx, argv + 1, argc - 1, callback->pd);
    ++cmdCtx->index;
    return REDISMODULE_OK;
}

static int CommandFilter_UnregisterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 2 || argc > 3){
        return RedisModule_WrongArity(ctx);
    }

    size_t cmdNameLen;
    const char* command = RedisModule_StringPtrLen(argv[1], &cmdNameLen);
    long long index = -1;

    char commandNameToLower[cmdNameLen + 1];
    for(size_t i = 0 ; i < cmdNameLen ; ++i){
        commandNameToLower[i] = tolower(command[i]);
    }
    commandNameToLower[cmdNameLen] = '\0';

    CommandCtx* cmdCtx = Gears_dictFetchValue(commandDict, commandNameToLower);
    if(!cmdCtx){
        RedisModule_ReplyWithError(ctx, "Given command does not exists");
        return REDISMODULE_OK;
    }

    if(argc == 3){
        if(RedisModule_StringToLongLong(argv[2], &index) != REDISMODULE_OK){
            RedisModule_ReplyWithError(ctx, "Could not parse index argument");
            return REDISMODULE_OK;
        }
        if(index < 0 || index >= array_len(cmdCtx->callbacks)){
            RedisModule_ReplyWithError(ctx, "Given index out of range");
            return REDISMODULE_OK;
        }
    }

    CommandInfoUnregisterCtx* cmdInfoUnregisterCtx = RG_ALLOC(sizeof(*cmdInfoUnregisterCtx));
    cmdInfoUnregisterCtx->name = RG_STRDUP(commandNameToLower);
    cmdInfoUnregisterCtx->index = index;

    char* err = NULL;
    FlatExecutionPlan* fep = RGM_CreateCtx(ShardIDReader, &err);
    if(!fep){
        if(!err){
            err = RG_STRDUP("Failed creating execution to unregister command");
        }
        CommandFilter_UnregisterArgFree(cmdInfoUnregisterCtx);
        RedisModule_ReplyWithError(ctx, "Failed creating execution to unregister command");
        RG_FREE(err);
        return REDISMODULE_ERR;
    }

    RGM_ForEach(fep, CommandFilter_UnregisterOnShard, cmdInfoUnregisterCtx);
    ExecutionPlan* ep = RedisGears_Run(fep, ExecutionModeAsync, NULL, NULL, NULL, mgmtWorker, &err);
    RedisGears_FreeFlatExecution(fep);
    if(!ep){
        if(!err){
            err = RG_STRDUP("Failed running execution to unregister command");
        }
        RedisModule_ReplyWithError(ctx, err);
        RG_FREE(err);
        return REDISMODULE_ERR;
    }

    RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    RedisGears_AddOnDoneCallback(ep, CommandFilter_SendReply, bc);
    RedisGears_AddOnDoneCallback(ep, CommandFilter_dropExecutionOnDone, NULL);

    return REDISMODULE_OK;
}

static int CommandFilter_DumpRegisteredCommands(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 1){
        return RedisModule_WrongArity(ctx);
    }

    if(!commandDict){
        RedisModule_ReplyWithArray(ctx, 0);
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithArray(ctx, Gears_dictSize(commandDict));

    Gears_dictIterator *iter = Gears_dictGetIterator(commandDict);
    Gears_dictEntry* entry = NULL;
    while((entry = Gears_dictNext(iter))){
        CommandCtx* cmdCtx = Gears_dictGetVal(entry);
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithCString(ctx, cmdCtx->name);

        RedisModule_ReplyWithArray(ctx, array_len(cmdCtx->callbacks));

        for(int i = 0 ; i < array_len(cmdCtx->callbacks) ; ++i){
            RedisModule_ReplyWithArray(ctx, 2);
            CallbackCtx* callback = cmdCtx->callbacks + i;
            RedisModule_ReplyWithCString(ctx, callback->callbackName);
            char* desc = callback->type->tostring(callback->pd);
            RedisModule_ReplyWithCString(ctx, desc);
            RG_FREE(desc);
        }
    }
    Gears_dictReleaseIterator(iter);


    return REDISMODULE_OK;
}

static void CommandFilter_FreeCallbackCtx(CallbackCtx* callbackCtx){
    callbackCtx->type->free(callbackCtx->pd);
    RG_FREE(callbackCtx->callbackName);
}

static void CommandFilter_FreeCommandCtx(CommandCtx* cmdCtx){
    RG_FREE(cmdCtx->name);
    for(size_t i = 0 ; i < array_len(cmdCtx->callbacks) ; ++i){
        CommandFilter_FreeCallbackCtx(cmdCtx->callbacks + i);
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


            callbackCtx.type = CommandsMgmt_GetArgType(callbackCtx.callbackName);
            callbackCtx.callback = CommandsMgmt_Get(callbackCtx.callbackName);

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

    if(Gears_dictSize(commandDict) > 0){
        CommandFilter_RegisterFilterIfNoRegitered();
    }

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

    FilterDataType = RedisModule_CreateDataType(ctx, COMMAND_FILTER_DATATYPE_NAME, COMMAND_FILTER_DATATYPE_VERSION, &methods);
}

int CommandFilter_CommandFilterInit(RedisModuleCtx* ctx){
    registerCommandArgType = RedisGears_CreateType("CommandFilterArg",
                                                  REGISTER_COMMAND_ARG_VERSION,
                                                  CommandFilter_RegisterArgFree,
                                                  CommandFilter_RegisterArgDuplicate,
                                                  CommandFilter_RegisterArgSerialize,
                                                  CommandFilter_RegisterArgDeserialize,
                                                  CommandFilter_RegisterArgToString);

    unregisterCommandArgType = RedisGears_CreateType("CommandFilterArg",
                                                  UNREGISTER_COMMAND_ARG_VERSION,
                                                  CommandFilter_UnregisterArgFree,
                                                  CommandFilter_UnregisterArgDuplicate,
                                                  CommandFilter_UnregisterArgSerialize,
                                                  CommandFilter_UnregisterArgDeserialize,
                                                  CommandFilter_UnregisterArgToString);

    RGM_RegisterForEach(CommandFilter_RegisterOnShard, registerCommandArgType);

    RGM_RegisterForEach(CommandFilter_UnregisterOnShard, unregisterCommandArgType);



    if (RedisModule_CreateCommand(ctx, "rg.unregistercommand", CommandFilter_UnregisterCommand, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.unregistercommand");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.dumpregisteredcommands", CommandFilter_DumpRegisteredCommands, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.unregistercommand");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, INNER_OVERIDE_COMMAND, CommandFilter_OverideCommand, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command "INNER_OVERIDE_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, INNER_OVERIDE_SLAVE_COMMAND, CommandFilter_OverideSlaveCommand, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command "INNER_OVERIDE_SLAVE_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, INNER_REGISTER_COMMAND_FILTER, CommandFilter_RegisterFilterInternal, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command "INNER_REGISTER_COMMAND_FILTER);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, INNER_UNREGISTER_COMMAND_FILTER, CommandFilter_UnregisterFilterInternal, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command "INNER_UNREGISTER_COMMAND_FILTER);
        return REDISMODULE_ERR;
    }

    overideCmdName = RedisModule_CreateString(NULL, INNER_OVERIDE_COMMAND, strlen(INNER_OVERIDE_COMMAND));

    commandDict = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);

    CommandFilter_RegisterDataType(ctx);

    return REDISMODULE_OK;
}
