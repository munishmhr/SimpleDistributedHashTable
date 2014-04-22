package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

	static String TAG = "SimpleDhtProvider";
	private enum Call {
		newJoini,setSucPre, insert, query,gdelete, delete,AllDumpCall,UpdateGDump,keyQuery,returnQueryCursor;
	}
	String currentNode;
	String call;
	static private ArrayList<String> old= new ArrayList<String>();
	static private ArrayList<String> new1;
	static private String succ = null;
	static private String pred = null;
	static private String key_e="key";
	static private String value_e="value";
	static private boolean small = false;
	static private boolean large = false;
	static String resultRow[]={key_e,value_e};	
	Context context;
	ContentResolver mContentResolver;
	static MatrixCursor mt = new MatrixCursor(resultRow); 
	static int mutex = 0;
	static int mutexi = 0;
	static int mutexa = 0;
	static String mutexQuery = null;
	private HashMap<String,String> mNameNodeMap;
	ArrayList<String> mNodeList;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		
		if(succ==null && pred==null){
			if(selection.equals("*")||selection.equals("@")){
				Log.d(TAG,"delete 1 ");
				String[] filesToDel = context.fileList();
				for (int i = 0; i < filesToDel.length; i++){
					context.deleteFile(filesToDel[i]);
				}
			} else{
				Log.d(TAG,"delete 2 ");
				context.deleteFile(selection);
			}			
		}

		if(selection.equals("@") && succ!=null){
			String[] filesToDel = context.fileList();
			for (int i = 0; i < filesToDel.length; i++){
				context.deleteFile(filesToDel[i]);
			}
		}
		if(selection.equals("*") && succ!=null){
			Log.d(TAG,"delete 3 ");
			String[] filesToDel = context.fileList();
			for (int i = 0; i < filesToDel.length; i++){
				context.deleteFile(filesToDel[i]);
			}
			Call call = Call.valueOf("delete");
			new Thread(new Client(call,succ,getPort())).start();
			//			while(mutexi==0){}
			//			mutexi=0;
		}

		if(selection.length()>1 && succ!=null){
			Log.d(TAG,"delete 4 ");
			boolean condition = true;
			String[] filesToDel = context.fileList();
			for (int i = 0; i < filesToDel.length; i++){
				if(selection.equals(filesToDel[i])){
					Log.d(TAG,"Delete  5 selection : "+selection +" filesToDel[i] "+filesToDel[i]);
					context.deleteFile(filesToDel[i]);
					condition = false;
					break;
				}
			}
			if(condition == true){
				Log.d(TAG,"delete 6");
				Call call = Call.valueOf("gdelete");
				new Thread(new Client(call,succ,selection+"."+getPort())).start();
				//				while(mutexa==0){}
				//				mutexa=0;
			}
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
		////Log.d(TAG,"inside insert 1 ");
		currentNode = getPort();
		// TODO Auto-generated method stub
		String key = (String) values.get("key");
		String value = (String) values.get("value");

		// If not alone in ring
		////Log.d(TAG,"inside insert 2 "+succ +"  "+pred+"  "+getPort());

		if(succ != null && pred != null){
			////Log.d(TAG,"inside insert 3 ");
			try {
				if((genHash(currentNode).compareTo(genHash(pred))<0)){ 
					////Log.d(TAG,"inside insert 4 ");
					if(genHash(key).compareTo(genHash(pred))>0 || genHash(key).compareTo(genHash(currentNode))<=0 ){
						Log.d(TAG,"Key Inserted at 5 "+getPort()+" Key: "+key+"value: "+value);
						insertKeyValue(key,value);	

					} else{
						////Log.d(TAG,"inside insert 6 ");
						Call call = Call.valueOf("insert");
						//Log.d(TAG,"Key forward at 6 by "+getPort()+"to succc: "+succ+" Key: "+key+"value: "+value+" msg call: "+call.name());
						new Thread(new Client(call,succ,key+"."+value)).start();
					}
				}else {
					////Log.d(TAG,"inside insert 7 ");
					if(genHash(key).compareTo(genHash(currentNode))<0 && genHash(key).compareTo(genHash(pred))>0){
						Log.d(TAG,"Key Inserted at 8  "+getPort()+" Key: "+key+"value: "+value);
						insertKeyValue(key,value);	
					}else{
						////Log.d(TAG,"inside insert 8 ");

						Call call = Call.valueOf("insert");
						//Log.d(TAG,"Key forward at 8 by "+getPort()+"to succc: "+succ+" Key: "+key+"value: "+value+" msg call: "+call.name());
						new Thread(new Client(call,succ,key+"."+value)).start();
					}
				}


			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}else if(succ == null && pred == null){
			try {
				Log.d(TAG,"Key Inserted at 10  "+getPort()+" Key: "+key+"value: "+value);
				insertKeyValue(key,value);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	void insertKeyValue(String key, String value2) throws IOException {
		FileOutputStream fos = context.openFileOutput(key,Context.MODE_PRIVATE);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		osw.write(value2);
		osw.flush();
		osw.close();
	}

	@Override

	public boolean onCreate() {
		Log.d(TAG, "onCreate():::");
		currentNode = getPort();
		context = getContext();
		mContentResolver = context.getContentResolver();
		Thread server = new Server();
		server.start();
		if(getPort().equals("5554")) {
			init();
		}
		return false;
	}

	void init() {
		mNameNodeMap = new HashMap<String, String>();
		mNodeList = new ArrayList<String>();
		try {
			mNodeList.add(genHash("5554"));
			mNameNodeMap.put(genHash("5554"), "5554");
			mNameNodeMap.put(genHash("5556"), "5556");
			mNameNodeMap.put(genHash("5558"), "5558");
			mNameNodeMap.put(genHash("5560"), "5560");
			mNameNodeMap.put(genHash("5562"), "5562");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	void clientSetSucPre(String msg){
		////Log.d(TAG,"inside clientSetSucPre 1 ");
		String ar[] = msg.split("\\.");
		String port = ar[2];
		Call call = Call.valueOf("setSucPre");
		new Thread(new Client(call,port,msg)).start();
	}

	void joinNode(String nodeToJoin) throws NoSuchAlgorithmException {
		Log.d(TAG, "joinNode()::" + nodeToJoin);

		String nodeId = genHash(nodeToJoin);
		if(!mNodeList.contains(nodeId)) {
			mNodeList.add(nodeId);
			Collections.sort(mNodeList);
		}
		Log.d(TAG, "joinNode()::" + mNodeList);

		if(mNodeList.size() == 1) {
			Log.d(TAG, "joinNode::size 1:returning");

			return;
		}

		int index = mNodeList.indexOf(nodeId);
		String succId = null;
		String predId = null;
		if(index == 0) {
			succId = mNodeList.get(index+1);
			predId = mNodeList.get(mNodeList.size()-1);
		} else if(index == (mNodeList.size()-1) ) {
			succId = mNodeList.get(0);
			predId = mNodeList.get(index-1);
		} else {
			succId = mNodeList.get(index+1);
			predId = mNodeList.get(index-1);
		}
		Log.d(TAG, "newNode:" + nodeToJoin + " pred:" + mNameNodeMap.get(predId) + " succ:" + mNameNodeMap.get(succId));
		//send to node
		//succ.pred.node.0.1
		clientSetSucPre(mNameNodeMap.get(succId) + "." + mNameNodeMap.get(predId) + "." + nodeToJoin + ".0.1"  );
		//send to succ
		clientSetSucPre("null" +"." + nodeToJoin  + "." + mNameNodeMap.get(succId) + ".0.1");
		//send to pred
		clientSetSucPre(nodeToJoin +"."+ "null" +"."+ mNameNodeMap.get(predId) + ".0.1");

	}

	class Server extends Thread{
		BufferedReader bR =null;
		ServerSocket serverSocket=null;
		Socket soc = null;
		String TAG;
		String inComing;
		String cas[];

		public void run(){
			Call call = Call.valueOf("newJoini");
			new Thread(new Client(call,"5554","")).start(); 		
			try{
				serverSocket = new ServerSocket(10000);
				while(true){

					soc = serverSocket.accept();
					bR = new BufferedReader(new InputStreamReader(soc.getInputStream()));
					inComing = bR.readLine();

					Log.d(TAG, "Server::run::incoming::" + inComing);
					cas = inComing.split("\\.");
					//Log.d(TAG,"Got req from : "+cas[1]);
					if(cas[0].equals("newJoini")) {
						Log.d(TAG,"server::run::Node join: "+cas[1]);
						joinNode(cas[1].trim());
					}

					if(cas[0].equals("setSucPre")){
						//Log.d(TAG,"Inside before setSucPre Switch Setting Succ "+succ+" and pred "+pred);
						if(!cas[1].equals("null")){
							succ = cas[1];
							//Log.d(TAG,"Setting Succ "+succ+"@ server port "+getPort());
						}

						if(!cas[2].equals("null")){
							pred = cas[2];
							//Log.d(TAG,"Setting Pred "+pred +"@ server port "+getPort());
						}
						if(cas[1].equals("nullset") || cas[2].equals("nullset")){
							succ = null;
							pred = null;
						}
						if(!cas[4].equals("2")) {
							if(cas[4].equals("0")){
								small = false;
							}else{
								small = true;
							}
							if(cas[5].equals("0")){
								large = false;
							}else{
								large = true;
							}

						}
						Log.d(TAG,"Inside setSucPre Switch Setting Succ "+succ+" and pred "+pred +" server port "+getPort() +"small : "+ small +" large : "+large);
					}
					if(cas[0].equals("insertCheck")){
						Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
						ContentValues cv = new ContentValues();
						cv.put("key", cas[1]);
						cv.put("value", cas[2]);
						mContentResolver.insert(mUri, cv);
						//Log.d(TAG,"Inside insertCheck getting msg pred and sending to succc inert check function :"+cas[1]+cas[2]);
					}
					if(cas[0].equals("@")){

							String portKeyValuePair[] = cas[1].split("-");
							String initiatorPort = portKeyValuePair[0];
							String oldkeyValuePair="";
							if(portKeyValuePair[1].length()>1){
								oldkeyValuePair = portKeyValuePair[1].substring(1);
							}
							String newPairs= "";
							if(!initiatorPort.equals(getPort())){
								newPairs = TakeLocalDump();
								Log.d(TAG,"All dump 1 newPairs deom local dump : "+newPairs);
								Call call1 = Call.valueOf("AllDumpCall");
								if(oldkeyValuePair.length()>0){
									new Thread(new Client(call1,succ,initiatorPort+"-;"+oldkeyValuePair+newPairs)).start();
								}else{
									new Thread(new Client(call1,succ,initiatorPort+"-"+oldkeyValuePair+newPairs)).start();
								}

								//Log.d(TAG,"Inside @ AllDumpCall called by * "+cas[0]+"  "+cas[1]);

							}else if(initiatorPort.equals(getPort())) {
								Log.d(TAG,"All dump 2 oldkeyValuePair "+oldkeyValuePair);
								if(oldkeyValuePair.length()>6){
									String keyVal[] = oldkeyValuePair.split(";");
									for(int i =0; i < keyVal.length ; i = i+2) {
										String key = keyVal[i];
										String value = keyVal[i+1];
										String[] row = { key, value };
										mt.addRow(row);
									}
									mutex = 1;
								}else{
									//check for delete
									Log.d(TAG,"All dump 3 empty cursor oldkeyValuePair : "+oldkeyValuePair);
									String row[] =  {,};
									MatrixCursor empty = new MatrixCursor(row);
									mt=empty;
									mutex = 1;
								}
							}
						//}
					}

					if(cas[0].equals("^")){
						new Thread(new Runnable(){
							@Override
							public void run() {
								// TODO Auto-generated method stub
								//keyport[0] = original port ; 
								//Log.d(TAG,"Inside ^ for key Query to original node"+cas[0]+"  "+cas[1]);
								String keyPort[] = cas[1].split(";");
								Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
								Cursor cursor = mContentResolver.query(mUri, null, keyPort[0], null, null);
								int valIndex = cursor.getColumnIndex("value");
								cursor.moveToFirst();
								String value = cursor.getString(valIndex);
								cursor.close();
								Call call = Call.valueOf("returnQueryCursor");
								new Thread(new Client(call,keyPort[1],value)).start();
							}
						}).start();
					}
					if(cas[0].equals("(")){
						//Log.d(TAG,"Inside ( for key Query returning to original node"+cas[0]+"  "+cas[1]);
						mutexQuery  = cas[1];
					}
					if(cas[0].equals("#")){
						String originator = cas[1];
						Log.d(TAG,"Inside # Code originator is :"+ originator);
						String[] filesToDel = context.fileList();
						if(!getPort().equals(originator)){
							for (int i = 0; i < filesToDel.length; i++){
								context.deleteFile(filesToDel[i]);
							}
							Call call4 = Call.valueOf("delete");
							new Thread(new Client(call4,succ,originator)).start();
						}
						else{		
							return;
						}
					}
					if(cas[0].equals("$")){
						Log.i(TAG,"dcsdcsdcsdcsdcsdcs"+cas[1]);
						//String sel[] = cas[1]
						String selection = cas[1];
						String originator = cas[2];
						Log.d(TAG,"Inside # Code originator is :"+originator);
						boolean condition = true;
						String[] filesToDel = context.fileList();
						if(!getPort().equals(originator)){
							Log.d(TAG,"Inside # Code originator is :"+originator);
							for (int i = 0; i < filesToDel.length; i++){
								if(selection.equals(filesToDel[i])){
									Log.d(TAG,"Inside # values equlas "+selection);
									context.deleteFile(filesToDel[i]);
									condition = false;
									mutexa=1;
									break;
								}
							}
							if(condition == true){
								Call call5 = Call.valueOf("gdelete");
								new Thread(new Client(call5,succ,selection+"."+originator)).start();
							}
						}else{
							return;
							//mutexa = 1;
						}
					}
				}

			}catch(Exception e){
				e.printStackTrace();
			}

		}

		private String TakeLocalDump() {
			String[] noOfFiles = context.fileList();
			String pair = "";
			for(int i =0 ;i<noOfFiles.length;i++){
				try {
					FileInputStream fin = context.openFileInput(noOfFiles[i]);
					InputStreamReader inpReader = new InputStreamReader(fin);
					BufferedReader br = new BufferedReader(inpReader);
					String value = br.readLine();
					pair = pair+";"+noOfFiles[i]+";"+value;
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
			return pair;
		}
	}

	class Client extends Thread{
		DataOutputStream dOS = null;
		Call call;
		String remotePort;
		String TAG;
		Socket clientSock;
		String msgToSend;
		String msg;
		int port1;
		Client(Call call,String remotePort,String msg){
			this.call = call;
			this.remotePort = remotePort;
			this.msg = msg;
		}
		public void run(){
			Log.d(TAG,"Client::thisPort="+getPort()+ " remotePort="  +remotePort + " msg=" + msg);
			Log.d(TAG, "Client::run::Call=" + call);
			try {
				port1 = Integer.parseInt(remotePort)*2;
				remotePort = port1+"";
				//Log.d(TAG,"Inside Client port is "+remotePort);
				clientSock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(remotePort.trim()));
				dOS = new DataOutputStream(clientSock.getOutputStream());

				switch(call){
				case newJoini :
					msgToSend = "newJoini"+"."+getPort();
					//Log.d(TAG," Inside client new Joini sending msg : "+msgToSend);
					dOS.writeBytes(msgToSend);
					//Log.d(TAG,"Inside Switch (new joine) Client port is "+getPort()+ "and msg has been send to server");
					break;
				case setSucPre :
					msgToSend = "setSucPre"+"."+msg;
					////Log.d(TAG,"check in cleinet for reply from node "+msgToSend);
					//Log.d(TAG," Inside client setSucPre sending msg : "+msgToSend);
					dOS.writeBytes(msgToSend);
					////Log.d(TAG,"Inside set client succ pree "+msgToSend);	
					break;

				case insert :
					msgToSend = "insertCheck"+"."+msg;
					////Log.d(TAG,"check in cleinet for reply from node "+msgToSend);
					//Log.d(TAG," Inside client insert sending msg : "+msgToSend);
					dOS.writeBytes(msgToSend);
					////Log.d(TAG,"Inside set client succ pree "+msgToSend);	
					break;
				case AllDumpCall :
					msgToSend = "@"+"."+msg;
					////Log.d(TAG,"check in cleinet for reply from node "+msgToSend);
					//Log.d(TAG," Inside client AllDumpCall sending msg : "+msgToSend);
					dOS.writeBytes(msgToSend);
					////Log.d(TAG,"Inside set client succ pree "+msgToSend);	
					break;
				case UpdateGDump:
					msgToSend = "&"+"."+msg;
					//Log.d(TAG," Inside client UpdateGDump sending msg : "+msgToSend);
					dOS.writeBytes(msgToSend);
					break;
				case keyQuery:
					msgToSend = "^"+"."+msg;
					//Log.d(TAG," Inside client keyQuery sending msg : "+msgToSend);
					dOS.writeBytes(msgToSend);
					break;
				case returnQueryCursor:
					msgToSend = "("+"."+msg;
					//Log.d(TAG," Inside client returnQueryCursor sending msg : "+msgToSend);
					dOS.writeBytes(msgToSend);
					break;

				case delete:
					msgToSend = "#"+"."+msg;
					//Log.d(TAG," Inside client returnQueryCursor sending msg : "+msgToSend);
					dOS.writeBytes(msgToSend);
					break;
				case gdelete:
					msgToSend = "$"+"."+msg;
					//Log.d(TAG," Inside client returnQueryCursor sending msg : "+msgToSend);
					dOS.writeBytes(msgToSend);
					break;

				default:
					break;
				}

			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally{
				try {
					dOS.close();
					clientSock.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		MatrixCursor localCur = new MatrixCursor(resultRow);
		String[] noOfFiles = context.fileList();
		String pair = "";
		if (succ == null && pred == null) {
			Log.i(TAG,"Query 1");
			if (selection.equals("*") || selection.equals("@")) {
				for (int i = 0; i < noOfFiles.length; i++) {
					try {
						Log.i(TAG,"Query 2");
						FileInputStream fin = context.openFileInput(noOfFiles[i]);
						InputStreamReader inpReader = new InputStreamReader(fin);
						BufferedReader br = new BufferedReader(inpReader);
						String value = br.readLine();
						String resultRow[] = { noOfFiles[i], value };
						localCur.addRow(resultRow);
						// Log.d(TAG," Inside query Zero 1 "+resultRow[0]+"  "+resultRow[1]);

					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				// Log.d(TAG," Inside query Zero 2 "+localCur.getCount());
				return localCur;
			} else {
				try {
					Log.i(TAG,"Query 3");
					// Log.d(TAG," Selection "+selection);
					FileInputStream fin = context.openFileInput(selection);
					InputStreamReader inpReader = new InputStreamReader(fin);
					BufferedReader br = new BufferedReader(inpReader);
					String value = br.readLine();
					String resultRow[] = { selection, value };
					// Log.d(TAG," Inside query Zero 3 succ and pred  after returning results and adding to cursor "+value
					// +" selection "+selection);
					localCur.addRow(resultRow);
					// Log.d(TAG,"cursor "+resultRow[0]
					// +" selection "+resultRow[1] + "  "+localCur.getCount());

					return localCur;
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					// Log.d(TAG,"inside exception : ");
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

		} else {
			if (selection.equals("*")) {
				Log.i(TAG,"Query 4");
				// Log.d(TAG," Inside query 1 : ");
				noOfFiles = context.fileList();
				if(noOfFiles.length > 0){
					for (int i = 0; i < noOfFiles.length; i++) {
						try {
							FileInputStream fin = context.openFileInput(noOfFiles[i]);
							InputStreamReader inpReader = new InputStreamReader(fin);
							BufferedReader br = new BufferedReader(inpReader);
							String value = br.readLine();

							pair = pair +";"+ noOfFiles[i] + ";" + value;
							Log.d(TAG," Inside query pair values for loop value for pair is : "+pair);
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					// cantatinating initiatores port
					pair = getPort() + "-" + pair;
					Log.i(TAG,"Query 6 "+pair);
					// Log.d(TAG," Inside query 3 outside loop value for originator port + pair is : "+pair);
					Call call = Call.valueOf("AllDumpCall");

					new Thread(new Client(call, succ, pair)).start();
					while (mutex == 0) {
					}
					mutex=0;
					return mt;
				}else{
					Call call = Call.valueOf("AllDumpCall");
					new Thread(new Client(call, succ, getPort()+"-;")).start();
					while (mutex == 0) {
					}
					mutex=0;
					return mt;
				}

			} else if (selection.equals("@")) {
				Log.i(TAG,"Query 7");
				noOfFiles = context.fileList();
				Log.d(TAG," Inside query 45 @ local dump "+ noOfFiles.length);
				for (int i = 0; i < noOfFiles.length; i++) {
					try {
						FileInputStream fin = context.openFileInput(noOfFiles[i]);
						InputStreamReader inpReader = new InputStreamReader(fin);
						BufferedReader br = new BufferedReader(inpReader);
						String value = br.readLine();
						Log.d(TAG,"Query 81 :"+value);
						String resultRow[] = { noOfFiles[i], value };
						Log.d(TAG,"Query 8 :"+noOfFiles[i]+"    "+value);
						mt.addRow(resultRow);

					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			} else if (!selection.equals("*") && !selection.equals("@")) {
				Log.i(TAG,"Query 8");
				if(succ != null && pred != null){
					////Log.d(TAG,"inside insert 3 ");
					try {
						if((genHash(getPort()).compareTo(genHash(pred))<0)){ 
							////Log.d(TAG,"inside insert 4 ");
							if(genHash(selection).compareTo(genHash(pred))>0 || genHash(selection).compareTo(genHash(getPort()))<=0 ){
								Log.i(TAG,"Query 9  ");
								//insertKeyValue(key,value);	
								FileInputStream fin = context.openFileInput(selection);
								InputStreamReader inpReader = new InputStreamReader(fin);
								BufferedReader br = new BufferedReader(inpReader);
								String value = br.readLine();
								String resultRow[] = { selection, value };
								localCur.addRow(resultRow);
								return localCur;
							} else{
								Log.i(TAG,"Query 10  ");
								Call call = Call.valueOf("keyQuery");
								new Thread(new Client(call,succ,selection+";"+getPort())).start();
								while(mutexQuery==null){};
								String row[] = {selection,mutexQuery};
								localCur.addRow(row);
								mutexQuery=null;
								return localCur;
							}
						}else {
							if(genHash(selection).compareTo(genHash(getPort()))<0 && genHash(selection).compareTo(genHash(pred))>0){
								Log.i(TAG,"Query 11  ");
								FileInputStream fin = context.openFileInput(selection);
								InputStreamReader inpReader = new InputStreamReader(fin);
								BufferedReader br = new BufferedReader(inpReader);
								String value = br.readLine();
								String resultRow[] = { selection, value };
								localCur.addRow(resultRow);
								return localCur;
							}else{
								Log.i(TAG,"Query 12  ");
								Call call = Call.valueOf("keyQuery");
								new Thread(new Client(call,succ,selection+";"+getPort())).start();
								while(mutexQuery==null){};
								String row[] = {selection,mutexQuery};
								localCur.addRow(row);
								mutexQuery=null;
								return localCur;
							}
						}
					} catch (NoSuchAlgorithmException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}else if(succ == null && pred == null){
					try {
						Log.i(TAG,"Query 13  ");
						FileInputStream fin = context.openFileInput(selection);
						InputStreamReader inpReader = new InputStreamReader(fin);
						BufferedReader br = new BufferedReader(inpReader);
						String value = br.readLine();
						String resultRow[] = { selection, value };
						localCur.addRow(resultRow);
						return localCur;
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			}
		}
		return mt;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
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

	public String getPort() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		return tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
}
