package common;

import common.exceptions.ProtocolException;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import common.utils.RecordReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Implementation of the wire protocol.
 */
public final class Protocol {

    private Protocol() {
    }

    private static final byte UNIT_SEPARATOR = 0x1f;

    /**
     * Encodes a {@link KVMessage} into binary format to transfer it over the network.
     *
     * @param message message to marshal
     * @return encoded data as per protocol
     */
    public static byte[] encode(KVMessage message) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        // add content type, static for now
        bos.write(ContentType.KV_MESSAGE);

        // PUT
        if (message.getStatus() == KVMessage.StatusType.PUT) {
            // op code
            bos.write(KVMessage.StatusType.PUT.opCode);

            // key
            byte[] keyData = message.putKey().getBytes(StandardCharsets.UTF_8);
            bos.write(keyData, 0, keyData.length);
            bos.write(UNIT_SEPARATOR);

            // value
            byte[] valueData = message.putValue().getBytes(StandardCharsets.UTF_8);
            bos.write(valueData, 0, valueData.length);
            bos.write(UNIT_SEPARATOR);

        //PUT_SUCCESS
        }else if(message.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
            // op code
            bos.write(KVMessage.StatusType.PUT_SUCCESS.opCode);

            // key
            byte[] keyData = message.putSuccessKey().getBytes(StandardCharsets.UTF_8);
            bos.write(keyData, 0, keyData.length);
            bos.write(UNIT_SEPARATOR);

            // value
            byte[] valueData = message.putSuccessValue().getBytes(StandardCharsets.UTF_8);
            bos.write(valueData, 0, valueData.length);
            bos.write(UNIT_SEPARATOR);

        //PUT_ERROR
        }else if(message.getStatus() == KVMessage.StatusType.PUT_ERROR) {
            // op code
            bos.write(KVMessage.StatusType.PUT_ERROR.opCode);

            // key
            byte[] keyData = message.putErrorKey().getBytes(StandardCharsets.UTF_8);
            bos.write(keyData, 0, keyData.length);
            bos.write(UNIT_SEPARATOR);

            // value
            byte[] valueData = message.putErrorValue().getBytes(StandardCharsets.UTF_8);
            bos.write(valueData, 0, valueData.length);
            bos.write(UNIT_SEPARATOR);

        //PUT_UPDATE
        }else if (message.getStatus() == KVMessage.StatusType.PUT_UPDATE ) {
            // op code
            bos.write(KVMessage.StatusType.PUT_UPDATE.opCode);

            // key
            byte[] keyData = message.putUpdateKey().getBytes(StandardCharsets.UTF_8);
            bos.write(keyData, 0, keyData.length);
            bos.write(UNIT_SEPARATOR);

            // value
            byte[] valueData = message.putUpdateValue().getBytes(StandardCharsets.UTF_8);
            bos.write(valueData, 0, valueData.length);
            bos.write(UNIT_SEPARATOR);

        //Get
        }else if(message.getStatus() == KVMessage.StatusType.GET) {
            // op code
            bos.write(KVMessage.StatusType.GET.opCode);

            // key
            byte[] keyData = message.getKey().getBytes(StandardCharsets.UTF_8);
            bos.write(keyData, 0, keyData.length);
            bos.write(UNIT_SEPARATOR);

            //value
            byte[] valueData = message.getValue().getBytes(StandardCharsets.UTF_8);
            bos.write(valueData, 0, valueData.length);
            bos.write(UNIT_SEPARATOR);

        //GET_ERROR
        }else if(message.getStatus() == KVMessage.StatusType.GET_ERROR) {
            // op code
            bos.write(KVMessage.StatusType.GET_ERROR.opCode);

            // key
            byte[] keyData = message.getKey().getBytes(StandardCharsets.UTF_8);
            bos.write(keyData, 0, keyData.length);
            bos.write(UNIT_SEPARATOR);

            //value
            byte[] valueData = message.getValue().getBytes(StandardCharsets.UTF_8);
            bos.write(valueData, 0, valueData.length);
            bos.write(UNIT_SEPARATOR);

        //GET_SUCCESS
        }else if(message.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
            // op code
            bos.write(KVMessage.StatusType.GET_SUCCESS.opCode);

            // key
            byte[] keyData = message.getKey().getBytes(StandardCharsets.UTF_8);
            bos.write(keyData, 0, keyData.length);
            bos.write(UNIT_SEPARATOR);

            //value
            byte[] valueData = message.getValue().getBytes(StandardCharsets.UTF_8);
            bos.write(valueData, 0, valueData.length);
            bos.write(UNIT_SEPARATOR);

        // DELETE
        }else if (message.getStatus() == KVMessage.StatusType.DELETE) {
            // op code
            bos.write(KVMessage.StatusType.DELETE.opCode);

            // key
            byte[] keyData = message.deleteKey().getBytes(StandardCharsets.UTF_8);
            bos.write(keyData, 0, keyData.length);
            bos.write(UNIT_SEPARATOR);

            // value
            byte[] valueData = message.deleteValue().getBytes(StandardCharsets.UTF_8);
            bos.write(valueData, 0, valueData.length);
            bos.write(UNIT_SEPARATOR);

        // DELETE_SUCCESS
        }else if (message.getStatus() == KVMessage.StatusType.DELETE_SUCCESS) {
            // op code
            bos.write(KVMessage.StatusType.DELETE_SUCCESS.opCode);

            // key
            byte[] keyData = message.deleteSuccessKey().getBytes(StandardCharsets.UTF_8);
            bos.write(keyData, 0, keyData.length);
            bos.write(UNIT_SEPARATOR);

            // value
            byte[] valueData = message.deleteSuccessValue().getBytes(StandardCharsets.UTF_8);
            bos.write(valueData, 0, valueData.length);
            bos.write(UNIT_SEPARATOR);

        // DELETE_ERROR
        }else if (message.getStatus() == KVMessage.StatusType.DELETE_ERROR) {
            // op code
            bos.write(KVMessage.StatusType.DELETE_ERROR.opCode);

            // key
            byte[] keyData = message.deleteErrorKey().getBytes(StandardCharsets.UTF_8);
            bos.write(keyData, 0, keyData.length);
            bos.write(UNIT_SEPARATOR);

            // value
            byte[] valueData = message.deleteErrorValue().getBytes(StandardCharsets.UTF_8);
            bos.write(valueData, 0, valueData.length);
            bos.write(UNIT_SEPARATOR);

        }else{

        }

        return bos.toByteArray();
    }

        // TODO: decide if we want to do this
//    /**
//     * Encodes an {@link ProtocolException} into binary format to transfer it over the network.
//     * @param exception exception to marshal
//     * @return encoded data as per protocol
//     */
//    public static byte[] encode(ProtocolException exception) {
//        // TODO
//        return null;
//    }

        /**
         * Decodes binary data as per protocol.
         * @param payload the binary data
         * @return the decoded message
         * @throws ProtocolException if the data actually encoded an exception
         */
        public static KVMessage decode(byte[] payload) throws ProtocolException {
            // TODO check and throw exception if length of payload < 3
            byte contentType = payload[0];

            if (contentType != ContentType.KV_MESSAGE) {
                throw new ProtocolException("Unsupported content type: " + contentType);
            }

            byte statusCode = payload[1];
            byte data[] = new byte[payload.length - 2];
            System.arraycopy(payload, 2, data, 0, data.length); //Maybe a problem with this code


            try {
                RecordReader reader = new RecordReader(data, UNIT_SEPARATOR);
                //PUT
                if (statusCode == KVMessage.StatusType.PUT.opCode) {
                    byte[] keyData = reader.read();
                    byte[] valueData = reader.read();
                    String key = new String(keyData, StandardCharsets.UTF_8);
                    String value = new String(valueData, StandardCharsets.UTF_8);

                    return new DefaultKVMessage(key, value, KVMessage.StatusType.PUT);

                //PUT_SUCCESS
                } else if (statusCode == KVMessage.StatusType.PUT_SUCCESS.opCode) {
                    byte[] keyData = reader.read();
                    byte[] valueData = reader.read();
                    String key = new String(keyData, StandardCharsets.UTF_8);
                    String value = new String(valueData, StandardCharsets.UTF_8);
                    return new DefaultKVMessage(key, value, KVMessage.StatusType.PUT_SUCCESS);

                //PUT_ERROR
                } else if (statusCode == KVMessage.StatusType.PUT_ERROR.opCode) {
                    byte[] keyData = reader.read();
                    byte[] valueData = reader.read();
                    String key = new String(keyData, StandardCharsets.UTF_8);
                    String value = new String(valueData, StandardCharsets.UTF_8);
                    return new DefaultKVMessage(key, value, KVMessage.StatusType.PUT_ERROR);

                //PUT_UPDATE
                }else if (statusCode == KVMessage.StatusType.PUT_UPDATE.opCode) {
                    byte[] keyData = reader.read();
                    byte[] valueData = reader.read();
                    String key = new String(keyData, StandardCharsets.UTF_8);
                    String value = new String(valueData, StandardCharsets.UTF_8);
                    return new DefaultKVMessage(key, value, KVMessage.StatusType.PUT_UPDATE);

                //GET
                }else if(statusCode == KVMessage.StatusType.GET.opCode) {
                    byte[] keyData = reader.read();
                    byte[] valueData = reader.read();
                    String key = new String(keyData, StandardCharsets.UTF_8);
                    String value = new String(valueData, StandardCharsets.UTF_8);
                    return new DefaultKVMessage(key, value, KVMessage.StatusType.GET);

                //GET_ERROR
                }else if(statusCode == KVMessage.StatusType.GET_ERROR.opCode) {
                    byte[] keyData = reader.read();
                    byte[] valueData = reader.read();
                    String key = new String(keyData, StandardCharsets.UTF_8);
                    String value = new String(valueData, StandardCharsets.UTF_8);
                    return new DefaultKVMessage(key, value, KVMessage.StatusType.GET_ERROR);

                //GET_SUCCESS
                }else if(statusCode == KVMessage.StatusType.GET_SUCCESS.opCode) {
                    byte[] keyData = reader.read();
                    byte[] valueData = reader.read();
                    String key = new String(keyData, StandardCharsets.UTF_8);
                    String value = new String(valueData, StandardCharsets.UTF_8);
                    return new DefaultKVMessage(key, value, KVMessage.StatusType.GET_SUCCESS);

                //DELETE
                }else if (statusCode == KVMessage.StatusType.DELETE.opCode) {
                    byte[] keyData = reader.read();
                    byte[] valueData = reader.read();
                    String key = new String(keyData, StandardCharsets.UTF_8);
                    String value = new String(valueData, StandardCharsets.UTF_8);
                    return new DefaultKVMessage(key, value, KVMessage.StatusType.DELETE);

                //DELETE_SUCCESS
                }else if (statusCode == KVMessage.StatusType.DELETE_SUCCESS.opCode) {
                    byte[] keyData = reader.read();
                    byte[] valueData = reader.read();
                    String key = new String(keyData, StandardCharsets.UTF_8);
                    String value = new String(valueData, StandardCharsets.UTF_8);
                    return new DefaultKVMessage(key, value, KVMessage.StatusType.DELETE_SUCCESS);

                //DELETE ERROR
                }else if (statusCode == KVMessage.StatusType.DELETE_ERROR.opCode) {
                    byte[] keyData = reader.read();
                    byte[] valueData = reader.read();
                    String key = new String(keyData, StandardCharsets.UTF_8);
                    String value = new String(valueData, StandardCharsets.UTF_8);
                    return new DefaultKVMessage(key, value, KVMessage.StatusType.DELETE_ERROR);

                }else{
                    throw new ProtocolException("Unsupported status code: " + statusCode);
                }
            } catch (IOException e) {
                throw new ProtocolException("Error decoding message.", e);
            }


        }


}
