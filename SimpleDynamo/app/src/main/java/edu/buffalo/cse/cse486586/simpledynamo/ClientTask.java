package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by akshadha on 4/24/17.
 */

public class ClientTask extends AsyncTask<String, Void, Void> {
    @Override
    protected Void doInBackground(String... msgs) {

        String msgToSend = msgs[0];

        try {

            Log.d("Client : ", "client task has been invoked");

//
            Log.d("Client/message:"," message is : " +msgToSend + " sending to : " +msgs[1]);

            if(msgToSend.contains("MESSAGETOINSERT")){

                Log.d("Client/messagetoinsert:"," message is : " +msgToSend + " sending to : " +msgs[1]);

                String msg1=msgToSend.split("MESSAGETOINSERT")[1];

                SimpleDynamoProvider.sendMessage(msgToSend,msgs[1]);

                SimpleDynamoProvider.msgCount= SimpleDynamoProvider.msgCount+1;
                Log.d("Client : ", "message has been sent to : " + SimpleDynamoProvider.msgCount +" ports" );

            }

            else if (msgToSend.contains("SINGLEQUERY")){

                String msg1= msgToSend.split("SINGLEQUERY")[1];

                Log.d("Client/singlequery : ", "message is : " +msgToSend + " sending to : " +msgs[1]);

                int check = SimpleDynamoProvider.sendMessage(msgToSend,msgs[1]);

                if(check == -1){
                    Log.d("Client/singlequery : ", "message is : " +msgToSend + " sending to dead : " +msgs[1]);
                    SimpleDynamoProvider.sendMessage(msgToSend,msgToSend.split("SINGLEQUERY")[3]);
                }

            }

            else if(msgToSend.contains("STARQUERY")) {

                Log.d("CLIENT/STAR QUERY : " , "Message contains star query :");

                String msg1 = msgToSend.split("STARQUERY")[0];

                Log.d("CLIENT : ", "VAlue array list size : " +SimpleDynamoProvider.valueArrayList.size() + SimpleDynamoProvider.valueArrayList.get(4));

                for(int i=0;i<SimpleDynamoProvider.valueArrayList.size();i++){

                    Log.d("CLIENT_A", " STAR QUERY MESSAGE1 : " + SimpleDynamoProvider.valueArrayList.get(i));

//                    if(!SimpleDynamoProvider.valueArrayList.get(i).equals(msgs[1])){
//                        SimpleDynamoProvider.sendMessage(msg1, SimpleDynamoProvider.valueArrayList.get(i));
//                    }
                }

                for(int i=0;i<SimpleDynamoProvider.valueArrayList.size();i++){

                    //Log.d("CLIENT_A", " STAR QUERY MESSAGE2 : " + msgToSend.split("STARQUERY")[0]);

                    if(!SimpleDynamoProvider.valueArrayList.get(i).equals(msgs[1])){
                        Log.d("CLIENT_A", " STAR QUERY MESSAGE1 : " + SimpleDynamoProvider.valueArrayList.get(i));
                        SimpleDynamoProvider.sendMessage(msgToSend, SimpleDynamoProvider.valueArrayList.get(i));
                    }


                }
            }

            else if(msgToSend.contains("MESSAGETODELETE")){

                Log.d("Client/msg2Delete:"," message is : " +msgToSend + " sending to : " +msgs[1]);

                String msg1=msgToSend.split("MESSAGETODELETE")[1];

                Log.d("CLIENT : ", "message sent to delete is : "+msgToSend );

                SimpleDynamoProvider.sendMessage(msgToSend,msgs[1]);

                SimpleDynamoProvider.msgCountDelete= SimpleDynamoProvider.msgCountDelete+1;
                Log.d("Client : ", "Delete message has been sent to : " + SimpleDynamoProvider.msgCountDelete +" ports" );

            }

            else if(msgToSend.contains("IAMBACK")){
                Log.d("Client/msg2Delete:"," message is : " +msgToSend + " sending to : " +msgs[1]);

                Log.d("CLIENT : ", "message sent to delete is : "+msgToSend );

                SimpleDynamoProvider.sendMessage(msgToSend,msgs[1]);
            }


        }catch(Exception e){
            Log.d("CLIENT / EXCEPTION"," message is : " +msgToSend + " sending to : " +msgs[1]);
            e.printStackTrace();
        }
        return null;
    }
}
