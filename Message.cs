using System.Text.Json;
using System.Text.Json.Serialization;

namespace mpi {
    public enum ServCall {
	Init = -1,
	Terminate = -2,
	Barrier = -3,
	Print = -4,
	Broadcast = -5
    }	

    public class Message {
	// these "header" fields identify the source and sink (sender and
	// recipient) of a message, and a "tag" which is used in sychron-
	// ization and a few other special cases.
	//
	public long Source { get; }
	public long Sink { get; }
	public long Tag { get; private set; }

	public object Text { get; }

	public Message (long source, long sink, object text, long tag = 0) {
	    Source = source;
	    Sink = sink;
	    Text = text;
	    Tag = tag;
	}

	// servoce methods that encode and decode messages
	//
	public string Encode() =>
	    JsonSerializer.Serialize(this);

	public static Message Decode(string encoded) =>
	    JsonSerializer.Deserialize<Message>(encoded)!;

	// a service method that converts the message text back into the
	// type the user specifies. MPI assumes that the user knows what
	// they are doing. I suppose we could pass type along--version 2
	// perhaps!
	//
//	public object ToType(Type t) =>
//	    JsonConvert.DeserializeObject(Text.ToString(), t);

	public T ToType<T>() =>
	    JsonSerializer.Deserialize<T>(Text.ToString())!;

	public override string ToString() =>
	    string.Format("{0}->{1}/{3}::{2}", Source, Sink, Text, Tag);
    }
}
