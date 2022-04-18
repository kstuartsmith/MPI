using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using System.Text.Json.Serialization;


namespace mpi
{
    class MPIServer {
	private static bool Debugging { get; set; }
	private static int NodeCount { get; set; }

	private static bool EnqueueForSink (Message msg) {
	    CommResources cr = CommResources.LocateCRById (msg.Sink);
	    if (cr != null) {
		cr.EnqueueOutgoing(msg);
	    }
	    return cr != null;
	}

	private static Dictionary<long, int> Barriers = new();
	private static void HandleOutOfBand(Message msg, CommResources cr) {
	    switch ((ServCall)msg.Sink) {
		case ServCall.Init: {
		    // this should be the first message...
		    cr.IAm = msg.Tag;
		    cr.EnqueueOutgoing(new Message((long)ServCall.Init, cr.IAm, "init ack", NodeCount));
		    break;
		}
		case ServCall.Terminate: {
		    // and this should be the last message...
		    break;
		}
		case ServCall.Barrier: {
		    // barrier synchronization
		    if (! Barriers.ContainsKey(msg.Tag))
			Barriers[msg.Tag] = NodeCount;
		    
		    Barriers[msg.Tag] -= 1;
		    if (Barriers[msg.Tag] == 0) {
			Barriers.Remove(msg.Tag);
			CommResources.Broadcast(new((long)ServCall.Barrier, -1, "barrier", msg.Tag), false);
		    }
		    break;
		}
		case ServCall.Print: {
		    // print
		    Console.WriteLine("[{0}]: {1}", msg.Source, msg.Text);
		    break;
		}
		case ServCall.Broadcast: {
		    // broadcast (enclude originator if Tag == 0)
		    CommResources.Broadcast(msg, msg.Tag == 0);
		    break;
		}
	    }
	}

	// this is "the" principal method. It establishes a server
	// that begins listening for node clients. When a client
	// connects, we launch a per-client server thread (PCST)
	// that will take over as the communicatios manager for
	// the client, moving message traffic to and from. 
	
	// The PCST establishes a read stream with the client to
	// receive message from the client destined for other
	// clients. Those messages are enqueued on the other clients'
	// message queues. PCST also sets up a write stream with
	// its client, through which PCST sends messages that are
	// it's client's message queue. Yes, a client can send itself
	// a message.
	//
	public static async Task startServerAsync(int servicePort) {

	    // this is the code executed by the PCST. Since the PCST
	    // in an independent thread, all local variable and any
	    // instantiations private/unshared, essentially encapsu-
	    // lated by the PCST.
	    //
	    void establishComm(object ncObj) {
		// the PCST is passed the connected nodeClient and
		// must set up the rest of the environment
		//
		var (nodeClient, nodeId) = (ValueTuple<TcpClient,long>)ncObj;
		CommResources cr = new(-1);

		// establish to/from streams... These will move encoded
		// messages only. Messages are enqueued/dequeued in
		// decoded form!
		
		var toNodeClient = new StreamWriter (nodeClient.GetStream()) {
		    AutoFlush = true
		};
		var fromNodeClient = new StreamReader (nodeClient.GetStream());

		List<Task> ioTasks = new();
		ioTasks.Add (Task.Run(async () => {
		    while (true) {
			string encoded = await fromNodeClient.ReadLineAsync();
			if (encoded == null)
			    break;
			Message msg
			    = JsonSerializer.Deserialize<Message>(encoded);
			if (Debugging)
			    Console.WriteLine ("Incoming: " + msg);
			if (msg.Sink < 0)
			    HandleOutOfBand(msg, cr);
			else
			    EnqueueForSink(msg);
		    }
		}));

		ioTasks.Add (Task.Run(async () => {
		    while (true) {
			Message msg = cr.DequeueOutgoing();
			if (Debugging)
			    Console.WriteLine ("Outgoing: " + msg);
			string encoded
			    = JsonSerializer.Serialize(msg);
			await toNodeClient.WriteLineAsync(encoded);
		    }
		}));

		Task.WaitAny (ioTasks.ToArray());
			
		CommResources.Remove(cr);
		
		nodeClient.Close();
		toNodeClient.Dispose();
		fromNodeClient.Dispose();

		Console.WriteLine("Node {0} dropped", cr.IAm);
	    }

	    // server main thread...

	    long connectionNbr = 0;
	    var server = new TcpListener(IPAddress.Any, servicePort);
	    server.Start();
	    Console.WriteLine ("establishing topology...");
	    while (true) {
		var nodeClient = await server.AcceptTcpClientAsync();
		Console.WriteLine ("connected {0}...", connectionNbr);
		ThreadPool.QueueUserWorkItem(establishComm, (nodeClient, connectionNbr++));
	    }
	}

	public static async Task Main (string[] args) {
	    Debugging = args.Length > 2 && args[2] == "-debug";
	    NodeCount = Int32.Parse(args[1]);

	    int servicePort = Int32.Parse(args[0]);
	    await MPIServer.startServerAsync(servicePort);
	}
    }
}
