package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {


    public String myPort = ""; //portStr * 2
    public String portStr;
    public String successorID = "";
    public String predecessorID = "";

    ArrayList<String> nodeslist = new ArrayList<String>();
    BlockingQueue<String> block = new ArrayBlockingQueue<String>(1);
    BlockingQueue<String> block2 = new ArrayBlockingQueue<String>(1);


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        try {
            String keyToInsert = values.get("key").toString();
            String valueToInsert = values.get("value").toString();
            Context context = getContext();

            String key_hash = genHash(keyToInsert);
            String portID_hash = genHash(portStr);
            String predecessorID_hash = genHash(predecessorID);


            if (successorID.equals(portStr) && predecessorID.equals(portStr))   //if there's only one node
            {
                FileOutputStream outputStream = context.openFileOutput(keyToInsert, Context.MODE_PRIVATE);
                Log.i("Insert:1 node ", "initiated key:" + keyToInsert);
                Log.i("Insert:1 node ", "initiated value:" + valueToInsert);
                outputStream.write(valueToInsert.getBytes());
                outputStream.close();


            }
            else if ((key_hash.compareTo(predecessorID_hash) > 0) && (key_hash.compareTo(portID_hash) < 0))     //if the node and predecessor are on the same side of 0 an d if key lies between node and successor
            {
                FileOutputStream outputStream = context.openFileOutput(keyToInsert, Context.MODE_PRIVATE);
                Log.i("Contentprovider", "check002 " + keyToInsert + " " + valueToInsert);
                outputStream.write(valueToInsert.getBytes());
                outputStream.close();


            } else if ((predecessorID_hash.compareTo(portID_hash) > 0) && ((portID_hash.compareTo(key_hash) > 0) && (predecessorID_hash.compareTo(key_hash)>0)))     //if the predecessor and the node lie on different sides of 0 and the key lies after 0 and before the node and the predecessor
            {
                FileOutputStream outputStream = context.openFileOutput(keyToInsert, Context.MODE_PRIVATE);
                Log.i("Contentprovider", "check003 " + keyToInsert + " " + valueToInsert);
                outputStream.write(valueToInsert.getBytes());
                outputStream.close();

            } else if ((predecessorID_hash.compareTo(portID_hash) > 0) && ((key_hash.compareTo(predecessorID_hash) > 0) && (predecessorID_hash.compareTo(key_hash)<0)))      //if the predecessor and the node lie on different sides of 0 and the key lies before 0 and after the node and the predecessor
            {
                FileOutputStream outputStream = context.openFileOutput(keyToInsert, Context.MODE_PRIVATE);
                Log.i("Contentprovider", "check004 " + keyToInsert + " " + valueToInsert);
                outputStream.write(valueToInsert.getBytes());
                outputStream.close();

            } else //forward the query to the successor
                {
                String send_insert_request = "Insert" + "###" + keyToInsert + "###" + valueToInsert ;
                Log.e("InsertForwardReqfrom", send_insert_request);
                new RequestForward().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send_insert_request);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        String readline = null;
        Context context = getContext();
        MatrixCursor matCur = new MatrixCursor(new String[]{"key", "value"});
        Log.e("QueryProcess", "Query initiated");
        Log.e("QueryProcess", selection);
        try {
            String[] list = context.fileList();
            Log.v("succ", successorID);
            Log.v("port",portStr);

            if ((selection.equals("*") && (successorID.equals(portStr))))   //if there's only one node and if selection is *
            {
                Log.e("QuerySelection", "Case *, one node");
                for (String file : list) {
                    InputStream inputStream = context.openFileInput(file);
                    if (inputStream != null) {
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader reader = new BufferedReader(inputStreamReader);
                        readline = reader.readLine();
                        String record[] = {file, readline};
                        Log.v("Queryfile", file);
                        Log.v("Queryvalue", record[1]);
                        matCur.addRow(record);
                    }
                }
                return matCur;
            } else if ((selection.equals("@")))     //if the selection is @
            {
                Log.e("QuerySelection", "Case @");
                for (String file : list) {
                    InputStream inputStream = context.openFileInput(file);
                    if (inputStream != null) {
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader reader = new BufferedReader(inputStreamReader);
                        readline = reader.readLine();
                        String record[] = {file, readline};
                        Log.v("Queryfile", file);
                        Log.v("Queryvalue", record[1]);
                        matCur.addRow(record);
                    }
                }
                return matCur;
            } else if (selection.equals("*") && (!successorID.equals(portStr)))     //if the selection is * and there's more than one node
            {
                //return all files from my node
                Log.e("QuerySelection", "Case: forward request for *");
                for (String file : list) {
                    InputStream inputStream = context.openFileInput(file);
                    if (inputStream != null) {
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader reader = new BufferedReader(inputStreamReader);
                        readline = reader.readLine();
                        String record[] = {file, readline};
                        Log.v("Queryfile", file);
                        Log.v("Queryvalue", record[1]);
                        matCur.addRow(record);
                    }
                }
                //query other nodes for their files
                String msg = "QueryAll###" + successorID + "###" + myPort;
                new RequestForward().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
                String allpairs = block.take();      //using a blocking queue to wait for the results of queries from other nodes
                String[] pairs = allpairs.split("::");       //splitting the string of keys,values which is of the form: key1<-->value1::key2<-->value2::key3<-->value3 and so on
                for (String i : pairs) {
                    if (!i.equals("") && !(i == null)) {
                        String[] keyval = i.split("<-->");
                        String key = keyval[0];
                        String value = keyval[1];
                        String record[] = {key, value};
                        matCur.addRow(record);
                    }
                }
                return matCur;
            }else if((!selection.equals("*")) && ((!selection.equals("@"))) && (successorID.equals(portStr)))       //if selection is the key and only one node
            {
                InputStream input = context.openFileInput(selection);
                if (input != null) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                    String record[] = {selection, reader.readLine()};
                    matCur.addRow(record);
                    Log.e("querykey1", record[0]);
                    Log.e("queryvalue1", record[1]);
                }
                return matCur;
            } else {                                         //if selection is the key and more than one nodes
                Log.e("querykeyselection", selection);
                String key_hash = genHash(selection);
                String portID_hash = genHash(portStr);
                String predecessorID_hash = genHash(predecessorID);
                Log.e("query:portstr",portStr);
                Log.e("query:pred",predecessorID);


                if ((portID_hash.compareTo(predecessorID_hash) > 0) && ((key_hash.compareTo(portID_hash) < 0) && (key_hash.compareTo(predecessorID_hash) > 0))) {
                    //key lies in that partition
                    InputStream input = context.openFileInput(selection);
                    if (input != null) {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                        String record[] = {selection, reader.readLine()};
                        matCur.addRow(record);
                        Log.e("querykey2", record[0]);
                        Log.e("queryvalue2", record[1]);
                        return matCur;
                    }
                    return matCur;
                } else if ((portID_hash.compareTo(predecessorID_hash) < 0) && (((key_hash.compareTo(predecessorID_hash) > 0) && (key_hash.compareTo(portID_hash) > 0)) || ((key_hash.compareTo(portID_hash) < 0) && (key_hash.compareTo(predecessorID_hash) < 0)))) {
                    //key lies in that partition
                    InputStream input = context.openFileInput(selection);
                    if (input != null) {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                        String record[] = {selection, reader.readLine()};
                        matCur.addRow(record);
                        Log.e("querykey3", record[0]);
                        Log.e("queryvalue3", record[1]);

                    }
                    return matCur;
                } else {                  //forward query request
                    String msg = "QueryKey###" + selection + "###" + myPort;
                    Log.e("QueryForward", msg);
                    new RequestForward().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

                    String returnedstring = block2.take();
                    Log.e("Returned string", returnedstring);

                    String[] keyval = returnedstring.split("<-->");
                    String key = keyval[0];
                    String value = keyval[1];
                    String record[] = {key, value};
                    matCur.addRow(record);

                    return matCur;
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    return null;

    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        Context context = getContext();

        //https://docs.oracle.com/javase/7/docs/api/java/io/File.html
        if(selection.equals("*") || selection.equals("@"))
        {
            File[] list  = context.getFilesDir().listFiles();
            for (File file : list) {
                file.delete();
            }
        }else{
            File dir =  context.getFilesDir();
            File file= new File(dir,selection);
            file.delete();
        }
        return 0;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    @Override
    public boolean onCreate() {
        Log.e("created port:", myPort);

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e("created port:", myPort);
        predecessorID = portStr;
        successorID = portStr;
        int server_port = 10000;
        try {
            ServerSocket serverSocket = new ServerSocket(server_port);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.e("OnCreate", "Server Task initiated");

        } catch (IOException e) {
            Log.e("OnCreate", "Can't create a server socket");
        }

        try {
            nodeslist.add(genHash(portStr));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        if ((!portStr.equals("5554"))) {

            String join_request = "JoinRequest###" + portStr;       //sending joining request to client thread
            Log.e("OnCreate", "sending join request:");
            new RequestForward().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, join_request);
        }
        return false;
    }

    private class ServerTask extends AsyncTask<ServerSocket, ArrayList, Void> {

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        Uri Uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            DataOutputStream outmessage = null;
            DataInputStream inmessage = null;
            String sendqueryout = "";
            String sendqueryout2 = "";
            String succ = "";
            String pred = "";
            String key;
            String value;
            Log.e("server", "check");
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    Log.e("server", "socket accepted");
                    inmessage = new DataInputStream(socket.getInputStream());
                    String inmsg = inmessage.readUTF();
                    String[] inputstuff = inmsg.split("###");

                    if (inputstuff[0].equals("JoinRequest"))        //5554 will ass a new node to the nodelist, sort it and send it to all avds on every new join
                    {
                        Log.e("server", "join loop");
                        nodeslist.add(genHash(inputstuff[1]));
                        Log.e("nodeslist size", String.valueOf(nodeslist.size()));
                        Collections.sort(nodeslist);
                        publishProgress(nodeslist);
                    }

                    if (inputstuff[0].equals("UpdateSuccPred"))         //All the ports will update their successor and predecessor on every new join
                    {
                        for (int i = 1; i < inputstuff.length; i++) {
                            if (portStr.equals(inputstuff[i])) {
                                if (i == (inputstuff.length - 1)) { //last  node
                                    predecessorID = inputstuff[i - 1];
                                    successorID = inputstuff[1];

                                } else if (i == 1) { //first node
                                    predecessorID = inputstuff[inputstuff.length - 1];
                                    successorID = inputstuff[i + 1];


                                } else {
                                    predecessorID = inputstuff[i - 1];
                                    successorID = inputstuff[i + 1];
                                }
                            }
                        }
                        Log.e("Updated Successor",successorID);
                        Log.e("Updated Predecessor", predecessorID);
                    }


                     else if (inputstuff[0].equals("Insert")) {        //successor will check for insert conditions in it's avd
                        ContentValues keyValuePair = new ContentValues();
                        keyValuePair.put("key", inputstuff[1]);
                        keyValuePair.put("value", inputstuff[2]);
                        insert(Uri, keyValuePair);

                    } else if (inputstuff[0].equals("QueryAll")) {       //successor will return all key, value pairs in it's memory and forward the query request

                        Cursor cursor2 = query(Uri, null, "@", null, null, null);
                        Log.e("ALLQueryServer","cursor created");

                        //https://stackoverflow.com/questions/30781603/how-to-retrieve-a-single-row-data-from-cursor-android
                        if (cursor2.moveToFirst()) {
                            do {

                                key = cursor2.getString(cursor2.getColumnIndex("key"));
                                value = cursor2.getString(cursor2.getColumnIndex("value"));
                                sendqueryout2 += key + "<-->" + value + "::";                           //converting the cursor to a string and seperating the key,value pairs using delimitors
                            } while (cursor2.moveToNext());
                        }
                        Log.e("ALLQueryServer",sendqueryout2);

                        String sendqueries = "StarQueryResults###" + sendqueryout2 + "###" + successorID;
                        Log.e("ALLQueryOut",sendqueries);
                        outmessage = new DataOutputStream(socket.getOutputStream());
                        outmessage.writeUTF(sendqueries);
                        outmessage.flush();

                    }
                    else if (inputstuff[0].equals("QueryKey")) {        //successor will look for the key in it's local storage and forward the request to other AVDs if not found

                        Cursor cursor = query(Uri, null, inputstuff[1], null, null, null);
                        Log.e("KeyQueryServer","cursor created");
                        if (cursor.moveToFirst()) {
                            do {
                                key = cursor.getString(cursor.getColumnIndex("key"));
                                value = cursor.getString(cursor.getColumnIndex("value"));
                            } while (cursor.moveToNext());
                            sendqueryout = key + "<-->" + value;
                            Log.e("KeyQueryServer",sendqueryout);
                        }
                        String sendqueries = "KeyQueryResults###" + sendqueryout + "###" + successorID;
                        outmessage = new DataOutputStream(socket.getOutputStream());
                        outmessage.writeUTF(sendqueries);
                        outmessage.flush();

                    }
                    socket.close();

                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        protected void onProgressUpdate(ArrayList... arr) {
            nodeslist = arr[0];
            String nodestring = "UpdateSuccPred###";
            for (String s : nodeslist) {
                nodestring += getIdFromHash(s) + "###";
            }
            Log.e("PublishProgressString", nodestring);
            new RequestForward().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodestring);  //5554 will send the updated nodelist to it's Client thread which will broadcast it to all other AVDs

        }
    }


    //Client Task
    public class RequestForward extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... string) {

            String req = string[0];
            String[] request = req.split("###");
            Socket socket = null;
            Log.e("Final Successor", successorID);
            Log.e("Final Predecessor", predecessorID);
            Log.e("Condition",request[0]);
//
            if (request[0].equals("JoinRequest")) {         //All AVDs except 5554 send a join request to 5554
                    Log.e("ClientProcessJoin", req);
                try {
                    //sending joining request to 5554
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 11108);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF(req);
                    out.flush();

                } catch (UnknownHostException e) {
                    Log.e("UnknownHost", "error");
                } catch (IOException e) {
                    Log.e("IOException", "5554 not found"); //Only one AVD alive
                }
            }


            try {
                if (request[0].equals("UpdateSuccPred")) {      //5554 will send the sorted nodelist to all other
//                    String sendme = "Client" + req;
                    Log.e("UpdateSuccPredClient", req);
                    Socket socket2 = null;
                    for (int i = 11108; i <= 11124; i = i+ 4) {
                        Log.e("Updating all: updating ",String.valueOf(i));
                        socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),i);
                        DataOutputStream update = new DataOutputStream(socket2.getOutputStream());
                        update.writeUTF(req);
                        socket2.close();
                        update.flush();
                        update.close();
                    }
                }
            } catch (UnknownHostException e) {
                Log.e("UnknownHost", "2");
            } catch (IOException e) {
                Log.e("IOException", "2");
                successorID = portStr;
                predecessorID = portStr;
            }catch (RuntimeException e) {
                Log.e("RuntimeException", "2");
            }
            try{

                if (request[0].equals("DeleteAll")) {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorID)*2);
                    DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
                    for_successor.writeUTF(req);
                }
                else if (request[0].equals("Insert")) {
                    Log.e("Client", "forwarding  insert request:");
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorID)*2);    //Forwarding insert request
                    DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
                    for_successor.writeUTF(req);
                }

                else if (request[0].equals("QueryAll")) {
                    String successor = successorID;
                    String originalSender = portStr;
                    Log.e(successor, originalSender);
                    String pairs = "";
                    Log.e("Query", "*");
                    while (!successor.equals(originalSender)) {
                        Log.e("QueryOriginPort", originalSender);
                        Log.e("Querying next: ", successor);
                        Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(successor)*2));    //forwarding query request
                        DataOutputStream for_successor = new DataOutputStream(socket2.getOutputStream());
                        for_successor.writeUTF(req);

                        DataInputStream inmessage = new DataInputStream(socket2.getInputStream());      //query results received at the node at which the query was initiated
                        String inmsg = inmessage.readUTF();
                        Log.e("QueryAll",inmsg);
                        String[] inputstuff = inmsg.split("###"); //"StarQueryResults",PAIRS, successor
//
                            successor = inputstuff[2];
                            pairs += inputstuff[1];
                            inmessage.close();
                            socket2.close();
                    }
                    Log.e("QueryAllPairs",pairs);
                    block.put(pairs);       //adding the string of all returned key,value pairs to the blocking queue
                }
                else if (request[0].equals("QueryKey")) {
                    Log.e("Forwarding Query to", successorID);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorID) * 2);
                    DataOutputStream query_forward = new DataOutputStream(socket.getOutputStream());
                    query_forward.writeUTF(req);

                    DataInputStream inmessage = new DataInputStream(socket.getInputStream());
                    String inmsg = inmessage.readUTF();
                    String[] inputstuff = inmsg.split("###"); //"KeyQueryResults",PAIRS, successor
                    Log.e("QueryClientPairs", inputstuff[1]);

                        String pair = inputstuff[1];
                        inmessage.close();
                        block2.put(pair);   //adding the string of the returned key,value pair to the blocking queue
//                    }
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e("UnknownHostException", "error");
            } catch (IOException e) {
                Log.e("IOException", "error");
            } catch (InterruptedException e) {
                Log.e("InterruptedException", "error");
            } catch (RuntimeException e){
//                Log.e("eRRor", "Runtime execption");
                e.printStackTrace();
            }


            return null;
        }
    }
            public String getIdFromHash(String hash) {
        String id = "";
        for (int i = 5554; i <= 5562; i += 2) {
            try {
                if ((genHash(String.valueOf(i)).trim()).equals((hash).trim())) {
                    id = String.valueOf(i);
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return id;
    }
}

