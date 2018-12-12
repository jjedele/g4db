package app_kvServer;

import common.messages.KVMessage;

public interface DataRequestHandler {

    KVMessage handle(KVMessage msg);

}
