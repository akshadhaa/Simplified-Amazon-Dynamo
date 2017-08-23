package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.apache.http.cookie.CookieAttributeHandler;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	public static String myPortBy2 = "";
	//ArrayList<String> targl = new ArrayList<String>();
	ArrayList<String> hashOfPort = new ArrayList<String>();

	public static TreeMap<String, String> tm;

	public static Set<String> keySet;

	public static ArrayList<String> keyArrayList;

	public static ArrayList<String> kl = new ArrayList<String>();

	public static ArrayList<String> valueArrayList;

	public static ArrayList<String> failedKeysList = new ArrayList<String>();

	public static HashMap<String, String> globalHashMap = new HashMap<String, String>();

	public static HashMap<String, String> globalHMQuery = new HashMap<String, String>();



	public static String leftPort1 = "";
	public static String leftPort2 = "";
	public static String rightPort1 = "";
	public static String rightPort2 = "";

	public static SharedPreferences sp;
	public static String t="";
	public static String targ="";
	public static String target123="";
	public static String[] targetList;
	public static String[] targetListQuery;

	public static boolean flag=false;

	public static int msgCount=0;

	public static int msgCountDelete=0;

	public static MatrixCursor globalMatrixCursor;

	public static Boolean errorFlag = false;

	public static HashMap<String, String> globalFailedMsgs =new HashMap<String, String>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String filterDelete=selection;

		try {

			String hashOfFilterDelete=genHash(filterDelete);
			Log.d("DELETE : ", "Delete has been called at : " + myPortBy2);

			String deleteTarget = targetPort(hashOfFilterDelete, myPortBy2);

			String[] deleteTargetList = deleteTarget.split("TARGET");


			for (int i = 0; i < deleteTargetList.length; i++) {
				String msgAtDelete = hashOfFilterDelete + "MESSAGETODELETE" + deleteTargetList[i] + "MESSAGETODELETE" + filterDelete + "MESSAGETODELETE"  + myPortBy2;

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgAtDelete, deleteTargetList[i]);
			}

			Log.d("Delete : ", "message count delete has been reset :" + "message has been deleted from 3 ports");


		}catch(NoSuchAlgorithmException e){
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		try {
			msgCount=0;

			String keyMsg = values.getAsString("key");
			String hashMsg = genHash(keyMsg);
			String msg = values.getAsString("value");
			Log.d("INSERT : ", "value of myport by 2 : " + myPortBy2);

			//calculateNeighbours(myPortBy2);
			t=targetPort(hashMsg, myPortBy2);

			Log.d("INSERT: ","value returned by target port ; 3 ports are: " +t + " for key: " +keyMsg );

			targetList=t.split("TARGET");



			for(int i=0;i<targetList.length;i++) {
				String msgAtInsert = hashMsg+"MESSAGETOINSERT"+targetList[i]+"MESSAGETOINSERT"+keyMsg +"MESSAGETOINSERT"+msg+"MESSAGETOINSERT"+myPortBy2;

//				try {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgAtInsert, targetList[i]);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				} catch (ExecutionException e) {
//					e.printStackTrace();
//				}
			}


            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Log.d("QUERY_A", "My exceptions");
            }

//			while(msgCount!=3){
//
//			}
//			msgCount=0;
			Log.d("Insert : ", "message count has been reset :" + "message has been sent to 3 ports");
			//Log.d("INSERT : ", "Target port returned by method is : " +target);
//			if(flag) {
//				Log.d("INSERT : ", "flag is set to true");
//				SharedPreferences.Editor edt = sp.edit();
//				edt.putString(keyMsg,msg);
//				edt.commit();
//				//keyList.add(values.getAsString("key"));
//			}
			// call targetPort method, with hashMsg as parameter
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return uri;
	}

	@Override
	public boolean onCreate() {

		// TODO Auto-generated method stub
		keyArrayList= new ArrayList<String>();
		valueArrayList= new ArrayList<String>();

		tm = new TreeMap<String, String>();

		String hashOfMyPortBy2 = "";
		SimpleDynamoProvider.sp = getContext().getSharedPreferences("hash_table_name", 4);

		valueArrayList.add("5562");
		valueArrayList.add("5556");
		valueArrayList.add("5554");
		valueArrayList.add("5558");
		valueArrayList.add("5560");

		try {
			for (int i = 0; i < valueArrayList.size(); i++) {
				hashOfPort.add(genHash(valueArrayList.get(i)));
				tm.put(genHash(valueArrayList.get(i)), valueArrayList.get(i));
				Log.d("OnCreate : ", "hash value of " + valueArrayList.get(i) + " is : " + hashOfPort.get(i));
				Log.d("VALUES OF ARRAYLIST : " ,valueArrayList.get(i));
			}

			keySet = tm.keySet();
			for (String str : keySet) {
				keyArrayList.add(str);
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Collections.sort(hashOfPort);



        Map<String,?> allEntries = sp.getAll();

        ServerTask.insertedKeysList.clear();
        for(String eachEntry: allEntries.keySet())
        {
            ServerTask.insertedKeysList.add(eachEntry);
        }

		try {
			TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
			myPortBy2 = String.valueOf((Integer.parseInt(portStr)));
			hashOfMyPortBy2 = genHash(myPortBy2);
			Log.d("OnCreate : ", "value of myport by 2 : " + myPortBy2);
			//	calculateNeighbours(myPortBy2);


			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);



			for(int i=0;i<valueArrayList.size();i++){
				if(!valueArrayList.get(i).equals(myPortBy2)){
					String msgAfterRejoin = myPortBy2+"IAMBACK";
					Log.d("oncreate :" ,"msg after rejoin "+msgAfterRejoin);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgAfterRejoin,valueArrayList.get(i));
				}
			}

		} catch (IOException e) {
			Log.d("OnCreate :", "Can't create a ServerSocket");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

        // Check this.


		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		String filter = selection;
		Log.d("QUERY : ", "THE KEY QUERIED IS : " + filter);

		if (filter.equals(("@"))) {
			MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});

			for (int i = 0; i < ServerTask.insertedKeysList.size(); i++) {
				Log.d("SDHT : QUERY", "Querying for all ");

				//  if(keyList.get(i).compareTo(genHash(myPortBy2))<0)

				matrixCursor.addRow(new String[]{ServerTask.insertedKeysList.get(i), sp.getString(ServerTask.insertedKeysList.get(i), "key")});
				Log.d("QUERY : @ ", "QUERY : KEY IS : " + ServerTask.insertedKeysList.get(i));
				Log.d("QUERY  : @", "QUERY : VALUE IS : " + sp.getString(ServerTask.insertedKeysList.get(i), "key"));

				// Map<String,String> keys= PreferenceManager.getDefaultSharedPreferences().getAll();
				// *****
				//Log.v("query", selection);
			}
			return matrixCursor;
		} else if (filter.equals("*")) {

			Log.d("QUERY*_A", "Case hit, star");

			globalMatrixCursor = new MatrixCursor(new String[]{"key", "value"});

			String queryMessageStar = myPortBy2 + "STARQUERY";

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMessageStar, myPortBy2);

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Log.d("QUERY_A", "My exceptions");
			}


			Log.d("QUERY*_A", "Others have populated their keys, I am adding mine.");

			for (int i = 0; i < ServerTask.insertedKeysList.size(); i++) {
				globalMatrixCursor.addRow(new String[]{ServerTask.insertedKeysList.get(i), sp.getString(ServerTask.insertedKeysList.get(i), "key")});
			}
			return globalMatrixCursor;
		} else {
			Log.d("QUERY : ", "SIMPLE QUERY CASE" + "value of filter is : " + filter);

			if (sp.contains(filter)) {
				String value = sp.getString(filter, "key");
				Log.d("QUERY_A", "I have the value for the key : " + value + "and i am " + myPortBy2);
				MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
				matrixCursor.addRow(new String[]{filter, value});
				return matrixCursor;
			} else {
				String tar1 = null;
				try {
					tar1 = targetPort(genHash(filter), myPortBy2);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				targetListQuery = tar1.split("TARGET");

				Log.d("QUERY / target : ", "target port is : " + targetListQuery[0] + " msg is : " + filter);

				String msgAtSingleQuery = filter + "SINGLEQUERY" + targetListQuery[0] + "SINGLEQUERY" + myPortBy2+ "SINGLEQUERY" + targetListQuery[1];



					Log.d("QUERY / for - loop : ", "target port is : " + targetListQuery[0] + " msg is : " + msgAtSingleQuery);

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgAtSingleQuery, targetListQuery[0]);

//                    if(globalHashMap.containsKey(filter))
//                        break;


				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					Log.d("QUERY_A", "My exceptions");
				}


				if (!globalHashMap.containsKey(filter)) {
					Log.d("Query : ", "Key is not present in global hashmap:" + filter);
				}

				MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
				matrixCursor.addRow(new String[]{filter, globalHashMap.get(filter)});
				return matrixCursor;

			}

		}
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}


	public String targetPort(String hashMessage, String myPortBy2 ) {

		ArrayList<String> keyArrayListCopy = new ArrayList<String>();
//call calculateneighbour method.
// compare hashmessage with hash(myportby2) ; and find the target by comparing with it's left1 and right1
		Log.d("TargetPort : " , "TargetPort has been called");
		for (int i = 0; i < keyArrayList.size(); i++) {
			keyArrayListCopy.add(keyArrayList.get(i));

		}

		keyArrayListCopy.add(hashMessage);
		Collections.sort(keyArrayListCopy);

		int msgIndex=keyArrayListCopy.indexOf(hashMessage);

		String targ = tm.get(keyArrayList.get((msgIndex)%5));

		String replica1 = tm.get(keyArrayList.get((msgIndex+1)%5));

		String replica2 = tm.get(keyArrayList.get((msgIndex+2)%5));

		target123=targ+"TARGET"+replica1+"TARGET"+replica2;


		Log.d("TargetPort : ", "message returned by target port : "+target123 );


//		catch (NoSuchAlgorithmException e){
//			e.printStackTrace();
//		}
		return target123;
	}

	public static void calculateNeighbours(String myPortBy2) {
		try {
			for (int i = 0; i < tm.size(); i++) {
				if (tm.get(keyArrayList.get(0)).equals((myPortBy2))) {
					rightPort1 = tm.get(keyArrayList.get(1));
					rightPort2 = tm.get(keyArrayList.get(2));
					leftPort1 = tm.get(keyArrayList.get(keyArrayList.size() - 1));
					leftPort2 = tm.get(keyArrayList.get(keyArrayList.size() - 2));
				} else if (tm.get(keyArrayList.get(4)).equals((myPortBy2))) {
					rightPort1 = tm.get(keyArrayList.get(0));
					rightPort2 = tm.get(keyArrayList.get(1));
					leftPort1 = tm.get(keyArrayList.get(keyArrayList.size() - 2));
					leftPort2 = tm.get(keyArrayList.get(keyArrayList.size() - 3));
				} else if (tm.get(keyArrayList.get(1)).equals((myPortBy2))) {
					rightPort1 = tm.get(keyArrayList.get(2));
					rightPort2 = tm.get(keyArrayList.get(3));
					leftPort1 = tm.get(keyArrayList.get(0));
					leftPort2 = tm.get(keyArrayList.get(keyArrayList.size() - 1));
				} else if (tm.get(keyArrayList.get(2)).equals((myPortBy2))) {
					rightPort1 = tm.get(keyArrayList.get(3));
					rightPort2 = tm.get(keyArrayList.get(4));
					leftPort1 = tm.get(keyArrayList.get(1));
					leftPort2 = tm.get(keyArrayList.get(0));
				} else if (tm.get(keyArrayList.get(3)).equals((myPortBy2))) {
					rightPort1 = tm.get(keyArrayList.get(4));
					rightPort2 = tm.get(keyArrayList.get(0));
					leftPort1 = tm.get(keyArrayList.get(2));
					leftPort2 = tm.get(keyArrayList.get(1));
				}
			}
			Log.d("Neighbours : ", "right port 1 : " + rightPort1);
			Log.d("Neighbours : ", "right port 2 : " + rightPort2);
			Log.d("Neighbours : ", "left port 1 : " + leftPort1);
			Log.d("Neighbours : ", "left port 2 : " + leftPort2);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}


	public static int sendMessage(String message, String port){
		String failedMsg="";
		String failedPort="";

//		ArrayList<String> localFailedKeysList = new ArrayList<String>();

		try {
			Log.d("SendMessage :", "sending message: "+message+"  to  " +port );
			Socket sock2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port) * 2);
			PrintWriter pw1 = new PrintWriter(new OutputStreamWriter(sock2.getOutputStream(), "UTF8"));

			BufferedReader br = new BufferedReader(new InputStreamReader(sock2.getInputStream()));


			pw1.write(message+"\n");
			pw1.flush();
			//pw1.close();


			String ackn = br.readLine();

			if(ackn==null){
				failedMsg = message;
				failedPort = port;
				Log.d("SendMessage: ", "failed message is : " +failedMsg);
				Log.d("SendMessage: ", "failed port is : " +failedPort);

				if(failedMsg.contains("MESSAGETOINSERT")){
					Log.d("SendMessage : ", "failed message contains MESSAGETOINSERT");
					String failedKey =failedMsg.split("MESSAGETOINSERT")[2];
					String failedValue = failedMsg.split("MESSAGETOINSERT")[3];
					globalFailedMsgs.put(failedKey,failedValue);
					failedKeysList = new ArrayList<String>(SimpleDynamoProvider.globalFailedMsgs.keySet());
					Log.d("SendMessage : ", "Key of globalfailedmsgs is : "+failedKey + " value is : " +failedValue);
				}
				else if(message.contains("SINGLEQUERY")){
					String secondPort=message.split("SINGLEQUERY")[3];
					sendMessage(message,secondPort);

				}

				Log.d("SendMessage : ", "Size of globalfailedmsessages : " +globalFailedMsgs.size());
				return -1;

			}

			else if (ackn.contains("ACK")) {
				Log.d("CLIENT : ", myPortBy2 + " has received ack");
			}
			sock2.close();
		}catch(IOException e){
			Log.d("EXCEPTION","port : " +port +" has failed");

			failedMsg = message;
			failedPort = port;
			Log.d("Provider / exception : ", failedPort +"has failed");
			Log.d("Provider/ exception : ", "failed msg is : " +failedMsg);
			if(failedMsg.contains("MESSAGETOINSERT")){
				String failedKey =failedMsg.split("MESSAGETOINSERT")[2];
				String failedValue = failedMsg.split("MESSAGETOINSERT")[3];
				globalFailedMsgs.put(failedKey,failedValue);
				failedKeysList = new ArrayList<String>(SimpleDynamoProvider.globalFailedMsgs.keySet());
			}
			else if(message.contains("SINGLEQUERY")){
				String secondPort=message.split("SINGLEQUERY")[3];
				sendMessage(message,secondPort);

			}
			Log.d("EXCEPTION","FAILS");
			e.printStackTrace();

			return -1;
		}

		return 0;
	}
}
