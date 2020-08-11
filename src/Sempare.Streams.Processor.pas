unit Sempare.Streams.Processor;

interface

uses
  Sempare.Streams.Filter,
  Sempare.Streams.Sort,
  System.Generics.Collections,
  Sempare.Streams.Types,
  Sempare.Streams.RttiCache,
  System.Generics.Defaults,
  System.Rtti;

// TODO
// 1. the processor 'cache' could be a bit smarter to accomodate filtering/sorting/counting
// 2. group by
// 3. map onto another stream processor
// 4. create a pipeline of actions...

type
  (* TStreamPipelineAction = class abstract
    function Get(const AOffset: integer): TValue; virtual; abstract;
    function Count: integer; virtual; abstract;
    end;

    TStreamPipelineSourcer = class(TStreamPipelineAction)

    end;

    TStreamPipelineFilter = class(TStreamPipelineAction)

    end;

    TStreamPipelineCache = class(TStreamPipelineAction)

    end;

    TStreamPipelineMapper = class(TStreamPipelineAction)

    end;

    TStreamPipelineOrderBy = class(TStreamPipelineAction)

    end;

    TStreamPipelineGroupBy = class(TStreamPipelineAction)

    end; *)

  TStreamProcessor = class;

  IStreamProcessor<T> = interface
    ['{CA312556-1BDD-4060-9940-352840CB2891}']

    procedure Apply(const AFunction: TApplyFunction<T>);
    function ToArray(): TArray<T>; overload;
    function ToList(): TList<T>; overload;
    function Count: integer;

    procedure Take(const ANumber: integer);
    procedure Skip(const ANumber: integer);

    function TakeOne: T;

    // state helper

    function Clone: IStreamProcessor<T>;
    function AsStreamProcessor: TStreamProcessor;

    // property helpers
    function GetFilter: IFilterProcessor;
    procedure SetFilter(const Value: IFilterProcessor);

    function GetRttiType: TRttiType;

    function GetSortExprs: TArray<TSortExpr>;
    procedure SetSortExprs(const Value: TArray<TSortExpr>);

    // properties
    property SortExpr: TArray<TSortExpr> read GetSortExprs write SetSortExprs;
    property RttiType: TRttiType read GetRttiType;
    property Filter: IFilterProcessor read GetFilter write SetFilter;
  end;

  TStreamProcessor = class abstract(TInterfacedObject)
  strict protected
    FValue: TValue;
    FFilter: IFilterProcessor;
    FType: TRttiType;
    FProcessed: TList<TValue>;
    FHasProcessed: boolean;
    FSortExpr: TArray<TSortExpr>;
    FSortCompare: IComparer<TValue>;

    FSkip, FTake: integer;
    procedure Invalidate;
  public
    constructor Create(const AValue: TValue; const AType: TRttiType);
    destructor Destroy; override;

    // core interface
    function Count: integer; virtual; abstract;
    function TakeOne<T>: T;
    function ToArray<T>(): TArray<T>; overload;
    function ToList<T>(): TList<T>; overload;
    procedure Take(const ANumber: integer);
    procedure Skip(const ANumber: integer);
    function Map<T, tout>(const AFunction: TMapFunction<T, tout>): TArray<tout>; overload;
    procedure Apply<T>(const AFunction: TApplyFunction<T>); overload;

    // utility

    function GetEnumerateAndFilterData(): TList<TValue>; virtual; abstract;
    function GetProcessed: TList<TValue>;
    procedure CopyFields(const ASource: TStreamProcessor);
    function AsStreamProcessor: TStreamProcessor;

    // utility

    class function ToArray<T>(const AData: TArray<TValue>): TArray<T>; overload; static;
    class function ToArray<T>(const AData: TList<TValue>): TArray<T>; overload; static;
    class function ToList<T>(const AData: TArray<TValue>): TList<T>; overload; static;
    class function ToList<T>(const AData: TList<TValue>): TList<T>; overload; static;
    class function Map<tin, tout>(const AData: TList<TValue>; const AFunction: TMapFunction<tin, tout>): TArray<tout>; overload; static;
    class procedure ApplyData<T>(const AData: TList<TValue>; const AFunction: TApplyFunction<T>); overload; static;

    // property helpers

    function GetSortExprs: TArray<TSortExpr>;
    procedure SetSortExprs(const Value: TArray<TSortExpr>);
    function GetRttiType: TRttiType;
    function GetFilter: IFilterProcessor;
    procedure SetFilter(const Value: IFilterProcessor);

    // properties

    property Value: TValue read FValue;
    property RttiType: TRttiType read GetRttiType;
    property Filter: IFilterProcessor read GetFilter write SetFilter;
    property SortExprs: TArray<TSortExpr> read GetSortExprs write SetSortExprs;
    property SortCompare: IComparer<TValue> read FSortCompare;
  end;

  TBaseStreamProcessor<T> = class abstract(TStreamProcessor, IStreamProcessor<T>)
  public
    constructor Create(const AValue: TValue);

    function Count: integer; override;
    function TakeOne: T; inline;
    function ToArray(): TArray<T>; overload; inline;
    function ToList(): TList<T>; overload; inline;

    function Clone: IStreamProcessor<T>; virtual; abstract;
    procedure Apply(const AFunction: TApplyFunction<T>); overload; inline;
  end;

  TArrayStreamProcessor<T> = class(TBaseStreamProcessor<T>)
  public
    function GetEnumerateAndFilterData(): TList<TValue>; override;
    function Clone: IStreamProcessor<T>; override;
  end;

  TListStreamProcessor<T> = class(TBaseStreamProcessor<T>)
  public
    function GetEnumerateAndFilterData(): TList<TValue>; override;
    function Clone: IStreamProcessor<T>; override;
  end;

  TEnumerableStreamProcessor<T> = class(TBaseStreamProcessor<T>)
  public
    function GetEnumerateAndFilterData(): TList<TValue>; override;
    function Clone: IStreamProcessor<T>; override;
  end;

implementation

{ TStreamProcessor }

function TStreamProcessor.AsStreamProcessor: TStreamProcessor;
begin
  result := self;
end;

procedure TStreamProcessor.CopyFields(const ASource: TStreamProcessor);
begin
  FSortCompare := ASource.FSortCompare;
  FValue := ASource.FValue;
  FFilter := ASource.FFilter;
  FType := ASource.FType;
  if ASource.FHasProcessed then
    FProcessed := TStreamProcessor.ToList<TValue>(ASource.FProcessed)
  else
    FProcessed := nil;
  FHasProcessed := ASource.FHasProcessed;
  FSortExpr := ASource.FSortExpr;
  FSkip := ASource.FSkip;
  FTake := ASource.FTake;
end;

constructor TStreamProcessor.Create(const AValue: TValue; const AType: TRttiType);
begin
  FValue := AValue;
  FType := AType;
  FHasProcessed := false;
  FProcessed := nil;
  FSkip := -1;
  FTake := -1;
end;

destructor TStreamProcessor.Destroy;
begin
  FFilter := nil;
  if FProcessed <> nil then
    FProcessed.Free;
  setlength(FSortExpr, 0);
  inherited;
end;

function TStreamProcessor.GetFilter: IFilterProcessor;
begin
  result := FFilter;
end;

function TStreamProcessor.GetProcessed: TList<TValue>;

begin
  if FHasProcessed then
    exit(FProcessed);
  FHasProcessed := true;
  FProcessed := GetEnumerateAndFilterData();
  if FSortCompare <> nil then
    FProcessed.Sort(FSortCompare);
  result := FProcessed;
end;

function TStreamProcessor.GetRttiType: TRttiType;
begin
  result := FType;
end;

function TStreamProcessor.GetSortExprs: TArray<TSortExpr>;
begin
  result := FSortExpr;
end;

procedure TStreamProcessor.Invalidate;
begin
  if FHasProcessed then
  begin
    FHasProcessed := false;
    FProcessed.Free;
    FProcessed := GetProcessed;
  end;
end;

procedure TStreamProcessor.SetFilter(const Value: IFilterProcessor);
begin
  FFilter := Value;
end;

procedure TStreamProcessor.SetSortExprs(const Value: TArray<TSortExpr>);
begin
  FSortExpr := Value;
  FSortCompare := TSortFieldComposite.Create(RttiType, Value);
  Invalidate;
end;

procedure TStreamProcessor.Skip(const ANumber: integer);
begin
  FSkip := ANumber;
  Invalidate;
end;

class function TStreamProcessor.ToArray<T>(const AData: TArray<TValue>): TArray<T>;
var
  v: TValue;
begin
  setlength(result, 0);
  for v in AData do
    insert(v.AsType<T>, result, length(result));
end;

class function TStreamProcessor.ToList<T>(const AData: TArray<TValue>): TList<T>;
var
  v: TValue;
begin
  result := TList<T>.Create;
  for v in AData do
    result.Add(v.AsType<T>());
end;

class function TStreamProcessor.Map<tin, tout>(const AData: TList<TValue>; const AFunction: TMapFunction<tin, tout>): TArray<tout>;
var
  v: TValue;
begin
  setlength(result, 0);
  for v in AData do
    insert(AFunction(v.AsType<tin>), result, length(result));
end;

procedure TStreamProcessor.Apply<T>(const AFunction: TApplyFunction<T>);
begin
  ApplyData<T>(GetEnumerateAndFilterData, AFunction);
end;

function TStreamProcessor.Map<T, tout>(const AFunction: TMapFunction<T, tout>): TArray<tout>;
begin
  result := Map<T, tout>(GetEnumerateAndFilterData, AFunction);
end;

class procedure TStreamProcessor.ApplyData<T>(const AData: TList<TValue>; const AFunction: TApplyFunction<T>);
var
  val: T;
  i: integer;
begin
  for i := 0 to AData.Count - 1 do
  begin
    val := AData[i].AsType<T>;
    AFunction(val);
    AData[i] := TValue.From<T>(val);
  end;
end;

procedure TStreamProcessor.Take(const ANumber: integer);
begin
  FTake := ANumber;
  Invalidate;
end;

function TStreamProcessor.TakeOne<T>: T;
begin
  FTake := 1;
  result := GetProcessed.Items[0].AsType<T>;
end;

function TStreamProcessor.ToArray<T>: TArray<T>;
begin
  result := ToArray<T>(GetProcessed);
end;

function TStreamProcessor.ToList<T>: TList<T>;
begin
  result := ToList<T>(GetProcessed);
end;

{ TArrayStreamProcessor }

function TArrayStreamProcessor<T>.Clone: IStreamProcessor<T>;
var
  r: TArrayStreamProcessor<T>;
begin
  r := TArrayStreamProcessor<T>.Create(FValue);
  r.CopyFields(self);
  result := r;
end;

function TArrayStreamProcessor<T>.GetEnumerateAndFilterData(): TList<TValue>;
var
  i: integer;
  v: TValue;
begin
  result := TList<TValue>.Create;
  for i := 0 to FValue.GetArrayLength - 1 do
  begin
    if (FSkip > 0) and (i < FSkip) then
      continue;
    if (FTake > 0) and (result.Count >= FTake) then
      exit;
    v := FValue.GetArrayElement(i);
    if (FFilter = nil) or FFilter.Filter(v) then
      result.Add(v);
  end;
end;

class function TStreamProcessor.ToArray<T>(const AData: TList<TValue>): TArray<T>;
var
  i: integer;
begin
  setlength(result, AData.Count);
  for i := 0 to AData.Count - 1 do
  begin
    result[i] := AData[i].AsType<T>;
  end;
end;

class function TStreamProcessor.ToList<T>(const AData: TList<TValue>): TList<T>;
var
  v: TValue;
begin
  result := TList<T>.Create();
  for v in AData do
    result.Add(v.AsType<T>());
end;

{ TListStreamProcessor<T> }

function TListStreamProcessor<T>.Clone: IStreamProcessor<T>;
var
  r: TListStreamProcessor<T>;
begin
  r := TListStreamProcessor<T>.Create(FValue);
  r.CopyFields(self);
  result := r;
end;

function TListStreamProcessor<T>.GetEnumerateAndFilterData(): TList<TValue>;
var
  l: TList<T>;
  val: T;
  i: integer;
  v: TValue;
begin
  l := FValue.AsObject as TList<T>;
  result := TList<TValue>.Create;
  for i := 0 to l.Count - 1 do
  begin
    if (FSkip > 0) and (i < FSkip) then
      continue;
    if (FTake > 0) and (result.Count >= FTake) then
      exit;
    v := TValue.From<T>(l[i]);
    if (FFilter = nil) or FFilter.Filter(v) then
      result.Add(v);
  end;
end;

{ TBaseStreamProcessor<T> }

function TBaseStreamProcessor<T>.Count: integer;
begin
  result := GetProcessed.Count;
end;

constructor TBaseStreamProcessor<T>.Create(const AValue: TValue);
begin
  inherited Create(AValue, Cache.GetType(typeinfo(T)));
end;

function TBaseStreamProcessor<T>.TakeOne: T;
begin
  result := inherited TakeOne<T>;
end;

function TBaseStreamProcessor<T>.ToArray: TArray<T>;
begin
  result := inherited ToArray<T>;
end;

function TBaseStreamProcessor<T>.ToList: TList<T>;
begin
  result := inherited ToList<T>;
end;

procedure TBaseStreamProcessor<T>.Apply(const AFunction: TApplyFunction<T>);
begin
  inherited Apply<T>(AFunction);
end;

{ TEnumerableStreamProcessor<T> }

function TEnumerableStreamProcessor<T>.Clone: IStreamProcessor<T>;
var
  r: TEnumerableStreamProcessor<T>;
begin
  r := TEnumerableStreamProcessor<T>.Create(FValue);
  r.CopyFields(self);
  result := r;
end;

function TEnumerableStreamProcessor<T>.GetEnumerateAndFilterData: TList<TValue>;
var
  l: IEnumerable<T>;
  e: IEnumerator<T>;
  val: T;
  i: integer;
  v: TValue;
begin
  l := IEnumerable<T>(FValue.AsInterface);
  result := TList<TValue>.Create;
  e := l.GetEnumerator;
  while e.MoveNext do
  begin
    if (FSkip > 0) and (i < FSkip) then
      continue;
    if (FTake > 0) and (result.Count >= FTake) then
      exit;
    v := TValue.From<T>(e.Current);
    if (FFilter = nil) or FFilter.Filter(v) then
      result.Add(v);
  end;
end;

end.
