package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

/**
 * Created by akshadha on 4/24/17.
 */

public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
    // public  Uri mUri= buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    String[] insertMsg1;
    public static ArrayList<String> insertedKeysList = new ArrayList<String>();

    public  Uri mUri= SimpleDynamoActivity.buildUri("content","edu.buffalo.cse.cse486586.simpledynamo.provider");

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];


        while (true) {

            try {

                Socket sock = serverSocket.accept();

                Log.d("Server : ", "Connection has been accepted ");


                BufferedReader br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                String message = br.readLine();

                Log.d("SERVER : " ,"message received is : " +message);
                PrintWriter pw = new PrintWriter(new OutputStreamWriter(sock.getOutputStream(), "UTF8"));


                if (message.contains("MESSAGETOINSERT")) {

                    pw.write("ACK" + "\n");
                    pw.flush();
                    //pw.close();


                    Log.d("SERVER : ", "message contains messagetoinsert");

                    insertMsg1 = message.split("MESSAGETOINSERT");


                    Log.d("inside insert in server", "value of key : " + insertMsg1[2]);





                    SharedPreferences.Editor edt = SimpleDynamoProvider.sp.edit();

                    edt.putString(insertMsg1[2], insertMsg1[3]);

                    edt.commit();

                    insertedKeysList.add(insertMsg1[2]);

                    SimpleDynamoProvider.globalHMQuery.put(insertMsg1[2],insertMsg1[3]);


                    Log.d("Server : ", "Inserted key :" + insertMsg1[2] + " ,value is :" + insertMsg1[3]);

                    SimpleDynamoProvider.flag = true;
                    sock.close();
                } else if (message.contains("SINGLEQUERY")) {
                    pw.write("ACK" + "\n");
                    pw.flush();

                    Log.d("SERVER : ", "QUERY IS FOR SIMPLE QUERY : ");

                    String queryMsg1 = message.split("SINGLEQUERY")[0];
                    String queryMsgPort = message.split("SINGLEQUERY")[1];
                    String returnItTo = message.split("SINGLEQUERY")[2];


                    Log.d("SERVER : ", "query is : " + queryMsg1 + "target is : " + queryMsgPort + " asked by:" + returnItTo);


                    if (SimpleDynamoProvider.sp.contains(queryMsg1)) {

                        Log.d("QUERY_A", "I have the response for you : " + queryMsg1);

                        String queryMsg2 = queryMsg1 + "QUERYMESSAGECASETWO2" + SimpleDynamoProvider.sp.getString(queryMsg1, "key");

                        SimpleDynamoProvider.sendMessage(queryMsg2, returnItTo);

                    }

                }

                else if (message.contains("QUERYMESSAGECASETWO2")) {

                    Log.d("SERVER : ","inside querymsgcase2 case");

                    pw.write("ACK" + "\n");
                    pw.flush();

                    String queryMsgKey = message.split("QUERYMESSAGECASETWO2")[0];
                    String queryMsgValue = message.split("QUERYMESSAGECASETWO2")[1];

                    Log.d("SERVER : Query case2 ", "key is : "+queryMsgKey);
                    Log.d("SERVER : Query case2 ", "value is : "+queryMsgValue);

                    SimpleDynamoProvider.globalHashMap.put(queryMsgKey, queryMsgValue);
                }

                else if (message.contains("STARQUERY")) {

                    pw.write("ACK" + "\n");
                    pw.flush();

                    String[] queryMsg4 = message.split("STARQUERY");
                    String responseQuery = "RESPONSETOSQUERY";

                    // SimpleDynamoProvider.kl = new ArrayList<String>(SimpleDynamoProvider.globalHashMap.keySet());

                    Log.d("QUERY_A", "STAR QUERY SENDER : " + queryMsg4[0]);

                    if (!queryMsg4[0].equals(SimpleDynamoProvider.myPortBy2)) {

                        for (int i = 0; i < insertedKeysList.size(); i++) {

                            String localKey = insertedKeysList.get(i);
                            String localValue = SimpleDynamoProvider.sp.getString(insertedKeysList.get(i), "key");

                            //Log.d("QUERY_A", "I have the value for the key : " + value + "and i am " + myPortBy2);
                            responseQuery += "@" + localKey + "&" + localValue;
                        }
                        SimpleDynamoProvider.sendMessage(responseQuery, queryMsg4[0]);

                    }
                }
                else if (message.contains("RESPONSETOSQUERY")) {

                    pw.write("ACK" + "\n");
                    pw.flush();

                    Log.d("QUERY_A", "RESPNSEQUERY : MESSAGE RECEIVED AT SERVER : " + message);

                    String[] messages = message.split("@");

                    for (int i = 1; i < messages.length; i++) {

                        Log.d("QUERY_A", "RESPNSEQUERY : MESSAGE RECEIVED AT SERVER : " + messages[i]);

                        String localKey = messages[i].split("&")[0];
                        String localValue = messages[i].split("&")[1];

                        Log.d("QUERY_A", "RESPONSEQUERY : KEY/VALUE : " + localKey + " : " + localValue);

                        SimpleDynamoProvider.globalMatrixCursor.addRow(new String[]{localKey, localValue});

                    }
                }

                if (message.contains("MESSAGETODELETE")) {

                    pw.write("ACK" + "\n");
                    pw.flush();
                    //pw.close();


                    Log.d("SERVER/Delete : ", "delete message contains: " +message);

                    String[] deleteMsg1 = message.split("MESSAGETODELETE");

                    Log.d("inside delete in server", "value of key : " + deleteMsg1[2]);

 //                   if(SimpleDynamoProvider.sp.contains(deleteMsg1[2])){
                        SharedPreferences.Editor edt = SimpleDynamoProvider.sp.edit();
//                        edt.remove(deleteMsg1[2]);
                        edt.clear();
                        edt.commit();
                        insertedKeysList.clear();
                        SimpleDynamoProvider.globalHMQuery.clear();
                 //   }
                    // sock.close();
                }

                else if(message.contains("IAMBACK")){
                    pw.write("ACK" + "\n");
                    pw.flush();
                    Log.d("SERVER : ", "Message contains IAMBACK.");


                    if(SimpleDynamoProvider.globalFailedMsgs.size()>0) {
                        String recoveryQuery = "RECOVERYQUERY";
                        String localKey = "";
                        String localValue = "";

                        String recoveryMsg = message.split("IAMBACK")[0];

                        for (int i = 0; i < SimpleDynamoProvider.globalFailedMsgs.size(); i++) {
                            localKey = SimpleDynamoProvider.failedKeysList.get(i);
                            localValue = SimpleDynamoProvider.globalFailedMsgs.get(localKey);

                            Log.d("Server : " ,"globalfailedmsgs : key : "+localKey+" value : "+localValue);

                            //Log.d("QUERY_A", "I have the value for the key : " + value + "and i am " + myPortBy2);
                            recoveryQuery += "@" + localKey + "&" + localValue;

                            Log.d("Server : ", "Recovery recoveryquery : "+recoveryQuery +" port" +recoveryMsg);

//                            SimpleDynamoProvider.globalFailedMsgs.remove(localKey);
//                            SimpleDynamoProvider.failedKeysList.remove(localKey);

                            Log.d("SERVER : ", "after recovery, size of globalfailedmsgs : " +SimpleDynamoProvider.globalFailedMsgs.size());

                        }

                        SimpleDynamoProvider.globalFailedMsgs.clear();
                        SimpleDynamoProvider.failedKeysList.clear();

                        SimpleDynamoProvider.sendMessage(recoveryQuery, recoveryMsg);

                    }
                }

                else if(message.contains("RECOVERYQUERY")) {

                    pw.write("ACK" + "\n");
                    pw.flush();

                    Log.d("Server:", "Recovery query : MESSAGE RECEIVED AT SERVER : " + message);

                    String[] messages = message.split("@");

                    for (int i = 1; i < messages.length; i++) {

                        Log.d("Server:", "recovery query : MESSAGE RECEIVED AT SERVER : " + messages[i]);

                        String localKey = messages[i].split("&")[0];
                        String localValue = messages[i].split("&")[1];

                        Log.d("Server", "Recovery query : KEY/VALUE : " + localKey + " : " + localValue);

                        //         SimpleDynamoProvider.sp = SimpleDynamoActivity.context.getSharedPreferences("hash_table_name", 4);


                        SharedPreferences.Editor edt = SimpleDynamoProvider.sp.edit();

                        edt.putString(localKey, localValue);

                        edt.commit();

                        if(!insertedKeysList.contains(localKey))
                            insertedKeysList.add(localKey);

                        SimpleDynamoProvider.globalHMQuery.put(localKey, localValue);

                        // SimpleDynamoProvider.globalMatrixCursor.addRow(new String[]{localKey, localValue});

                    }
                }

            }
            catch (IOException e)

            {
                e.printStackTrace();
            }

        }

    }
}
