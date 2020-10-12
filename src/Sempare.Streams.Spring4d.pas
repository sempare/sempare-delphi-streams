unit Sempare.Streams.Spring4d;

interface

{$I 'Sempare.Streams.inc'}
{$IF defined(SEMPARE_STREAMS_SPRING4D_SUPPORT)}

uses
  Spring.Collections,
  Sempare.Streams;

type
  Stream = Sempare.Streams.Stream;

  StreamSpring4dHelper = record helper for Stream

    /// <summary>
    /// Stream from a IEnumerable&lt;T&gt; source.
    /// </summary>
    /// <param name="ASource">A source of type IEnumerable&lt;T&gt;.</param>
    /// <returns>TStreamOperation&lt;T&gt; allowing additional operations on the IEnumerable source.</returns>
    class function From<T>(ASource: Spring.Collections.IEnumerable<T>): TStreamOperation<T>; overload; static;
  end;
{$ENDIF}

implementation

{$IF defined(SEMPARE_STREAMS_SPRING4D_SUPPORT)}

uses
  Sempare.Streams.Enum;

class function StreamSpring4dHelper.From<T>(ASource: Spring.Collections.IEnumerable<T>): TStreamOperation<T>;
begin
  result := TSpringIEnumerableEnum<T>.Create(ASource);
end;

{$ENDIF}

end.
