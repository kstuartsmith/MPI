using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace mpi {
    public class CommResources {
	// this class is essentially a collection of information used to
	// facilitate (but not perform) communication between clients by
	// providing a message queue and supporting methods.
	//
	// Within the context of MPI, every node in the topology has one
	// instance of CommResources bound to it through its per-client
	// service thread (see MPIServer).
	//
	// NONE of this should be even visible, much less necessary for
	// an application-level user.

	private Queue<Message> incomingQueue;
	private Queue<Message> outgoingQueue;

	// we need to be able to scroll through instances of the Comm-
	// Resources to find the instance servicing targeted message
	// recipients. This gets set by the server when the PCST is
	// launched.

	public long IAm { get; set; }

	// Enqueue an (unencoded) message. We update the queue and
	// throw the commNQedEvent in case the PCST is waiting.
	//
	public void EnqueueIncoming(Message msg) {
	    lock(incomingQueue) {
		incomingQueue.Enqueue(msg);
		Monitor.PulseAll(incomingQueue);
	    }
	}

	// not a whole lot to say here: either we get a message
	// from the thread-safe queue... or we don't (null).
	//
	public Message DequeueIncoming() {
	    Message msg;
	    lock(incomingQueue) {
		while (incomingQueue.Count == 0)
		    Monitor.Wait(incomingQueue);
		msg = incomingQueue.Dequeue();
	    }
	    return msg;
	}

	public void EnqueueOutgoing(Message msg) {
	    lock(outgoingQueue) {
		outgoingQueue.Enqueue(msg);
		Monitor.PulseAll(outgoingQueue);
	    }
	}

	public Message DequeueOutgoing() {
	    Message msg;
	    lock (outgoingQueue) {
		while (outgoingQueue.Count == 0)
		    Monitor.Wait(outgoingQueue);
		msg = outgoingQueue.Dequeue();
	    }
	    return msg;
	}
	
	private static ConcurrentBag<CommResources> commResources = new();
	public static void Remove(CommResources gone) {
	    commResources.TryTake(out gone);
	}
	
	public static CommResources LocateCRById (long id) {
	    foreach (var cr in commResources)
		if (id == cr.IAm)
		    return cr;
	    return null;
	}

	public static void Broadcast (Message msg, bool exclude) {
	    foreach (var cr in commResources) {
		if ((!exclude) || cr.IAm != msg.Source)
		    cr.EnqueueOutgoing(new(msg.Source, cr.IAm, msg.Text, msg.Tag));
	    }
	}

	public CommResources(long whoIAm) {
	    incomingQueue = new();
	    outgoingQueue = new();
	    
	    IAm = whoIAm;

	    commResources.Add(this);
	}
    }
}
