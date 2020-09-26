unit Sempare.Streams.Enum;

interface

uses
  System.Generics.Defaults,
  System.Generics.Collections,
  Sempare.Streams.Types,
  Sempare.Streams.Filter;

type
  Enum = class
  public
    class function Count<T>(AEnum: IEnum<T>): integer; static;
    class function ToArray<T>(AEnum: IEnum<T>): TArray<T>; static;
    class function ToList<T>(AEnum: IEnum<T>): TList<T>; static;
    class procedure Apply<T>(AEnum: IEnum<T>; const AFunc: TApplyFunction<T>); static;
    class function IsCached<T>(AEnum: IEnum<T>): boolean; static;
    class function TryGetCached<T>(AEnum: IEnum<T>; out ACachedEnum: IEnum<T>): boolean; static;
    class function Cache<T>(AEnum: IEnum<T>): IEnum<T>; overload; static;
    class function Cache<T>(AEnum: TList<T>): IEnum<T>; overload; static;
    class function Cache<T>(AEnum: TEnumerable<T>): IEnum<T>; overload; static;
    class function Map<T, TOutput>(AEnum: IEnum<T>; const AFunction: TMapFunction<T, TOutput>): TArray<TOutput>; static;

    class function GroupBy<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, T>; overload; static;
    class function GroupBy<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TValueType>; overload; static;
    class function GroupToLists<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, TList<T>>; overload; static;
    class function GroupToLists<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TList<TValueType>>; overload; static;
    class function GroupToArray<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, TArray<T>>; overload; static;
    class function GroupToArray<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TArray<TValueType>>;
      overload; static;
  end;

  THasMore<T> = class abstract(TInterfacedObject, IEnum<T>)
  private
    FFirst: boolean;
  public
    constructor Create;
    function EOF: boolean; virtual; abstract;
    procedure Next; virtual; abstract;
    function Current: T; virtual; abstract;
    function HasMore: boolean;
  end;

  TArrayEnum<T> = class(THasMore<T>)
  private
    FData: TArray<T>;
    FOffset: integer;
  public
    constructor Create(const AData: TArray<T>);
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

  TEnumerableEnum2<T> = class(THasMore<T>)
  private
    FEnum: TEnumerator<T>;
    FEof: boolean;
  public
    constructor Create(const AEnum: TEnumerable<T>);
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

  TEnumerableEnum<T> = class(THasMore<T>)
  private
    FEnum: IEnumerator<T>;
    FEof: boolean;
  public
    constructor Create(const AEnum: IEnumerable<T>);
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

  TBaseEnum<T> = class abstract(THasMore<T>)
  protected
    FEnum: IEnum<T>;
  public
    constructor Create(AEnum: IEnum<T>);
    destructor Destroy; override;
    function EOF: boolean; override;
    procedure Next; override;
    function Current: T; override;
  end;

  TSortedEnum<T> = class(THasMore<T>)
  private
    FItems: TList<T>;
    FEnum: IEnum<T>;
  public
    constructor Create(Enum: IEnum<T>; comparator: IComparer<T>);
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

  TUnionEnum<T> = class(THasMore<T>)
  private
    FEnums: TArray<IEnum<T>>;
    FIdx: integer;
  public
    constructor Create(Enum: TArray < IEnum < T >> );
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

  TFilterEnum<T> = class(TBaseEnum<T>)
  private
    FFilter: IFilterFunction<T>;
    FNext: T;
    FHasValue: boolean;
  public
    constructor Create(AEnum: IEnum<T>; AFilter: TFilterFunction<T>); overload;
    constructor Create(AEnum: IEnum<T>; AFilter: IFilterFunction<T>); overload;
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

  TSkip<T> = class(TBaseEnum<T>)
  public
    constructor Create(AEnum: IEnum<T>; ASkip: integer);
  end;

  TTake<T> = class(TBaseEnum<T>)
  private
    FTake: integer;
    FEof: boolean;
  public
    constructor Create(AEnum: IEnum<T>; ATake: integer);
    function EOF: boolean; override;
    procedure Next; override;
  end;

  TApplyEnum<T> = class(TBaseEnum<T>)
  private
    FApply: TApplyFunction<T>;
  public
    constructor Create(AEnum: IEnum<T>; const AApply: TApplyFunction<T>);
    function Current: T; override;
  end;

  TEnumCache<T> = class(TInterfacedObject, IEnumCache<T>)
  private
    FCache: TList<T>;
    FOwn: boolean;
  public
    constructor Create(AEnum: TList<T>); overload;
    constructor Create(AEnum: TEnumerable<T>); overload;
    constructor Create(AEnum: IEnum<T>; const AOwn: boolean = false); overload;
    destructor Destroy; override;
    function GetCache: TList<T>;
    function GetEnum: IEnum<T>;
  end;

  TCachedEnum<T> = class(THasMore<T>, IEnumCache<T>)
  private
    FCache: IEnumCache<T>;
    FEnum: IEnum<T>;
  public
    constructor Create(Cache: IEnumCache<T>);
    destructor Destroy; override;
    function EOF: boolean; override;
    procedure Next; override;
    function Current: T; override;
    function GetCache: TList<T>;
    function GetEnum: IEnum<T>;
  end;

  TMapEnum<TInput, TOutput> = class(THasMore<TOutput>)
  private
    FEnum: IEnum<TInput>;
    FMapper: TMapFunction<TInput, TOutput>;
  public
    constructor Create(AEnum: IEnum<TInput>; AMapper: TMapFunction<TInput, TOutput>);
    destructor Destroy; override;
    function Current: TOutput; override;
    function EOF: boolean; override;
    procedure Next; override;
  end;

  TJoinEnum<TLeft, TRight, TJoined> = class(THasMore<TJoined>)
  private
    FEnumLeft: IEnum<TLeft>;
    FEnumRight: IEnum<TRight>;
    FOn: TJoinOnFunction<TLeft, TRight>;
    FSelect: TJoinSelectFunction<TLeft, TRight, TJoined>;
    FHasLeft, FHasRight: boolean;
    FNext: TJoined;
    FHasNext: boolean;
    procedure FindNext;
    procedure ResetRight;
  public
    constructor Create( //
      AEnumLeft: IEnum<TLeft>; AEnumRight: IEnum<TRight>; //
      const AOn: TJoinOnFunction<TLeft, TRight>; const ASelect: TJoinSelectFunction<TLeft, TRight, TJoined>);
    destructor Destroy; override;
    function Current: TJoined; override;
    function EOF: boolean; override;
    procedure Next; override;
  end;

  TLeftJoinEnum<TLeft, TRight, TJoined> = class(THasMore<TJoined>)
  private
    FEnumLeft: IEnum<TLeft>;
    FEnumRight: IEnum<TRight>;
    FOn: TJoinOnFunction<TLeft, TRight>;
    FSelect: TJoinSelectFunction<TLeft, TRight, TJoined>;
    FHasLeft, FHasRight: boolean;
    FNext: TJoined;
    FHasNext: boolean;
    FFoundRight: boolean;
    procedure FindNext;
    procedure ResetRight;
  public
    constructor Create( //
      AEnumLeft: IEnum<TLeft>; AEnumRight: IEnum<TRight>; //
      const AOn: TJoinOnFunction<TLeft, TRight>; const ASelect: TJoinSelectFunction<TLeft, TRight, TJoined>);
    destructor Destroy; override;
    function Current: TJoined; override;
    function EOF: boolean; override;
    procedure Next; override;
  end;

implementation

uses
  System.SysUtils,
  System.Rtti,
  Sempare.Streams.Rtti;

{ TArrayEnum<T> }

constructor TArrayEnum<T>.Create(const AData: TArray<T>);
begin
  inherited Create();
  FData := AData;
end;

function TArrayEnum<T>.Current: T;
begin
  result := FData[FOffset];
end;

function TArrayEnum<T>.EOF: boolean;
begin
  result := FOffset = length(FData);
end;

procedure TArrayEnum<T>.Next;
begin
  if EOF then
    exit;
  inc(FOffset);
end;

{ TEnumerableEnum<T> }

constructor TEnumerableEnum<T>.Create(const AEnum: IEnumerable<T>);
begin
  inherited Create();
  FEnum := AEnum.GetEnumerator;
  Next;
end;

function TEnumerableEnum<T>.Current: T;
begin
  result := FEnum.Current;
end;

destructor TEnumerableEnum<T>.Destroy;
begin
  FEnum := nil;
  inherited;
end;

function TEnumerableEnum<T>.EOF: boolean;
begin
  result := FEof;
end;

procedure TEnumerableEnum<T>.Next;
begin
  FEof := not FEnum.MoveNext;
end;

{ TBaseEnum<T> }

constructor TBaseEnum<T>.Create(AEnum: IEnum<T>);
begin
  inherited Create();
  FEnum := AEnum;
end;

function TBaseEnum<T>.Current: T;
begin
  result := FEnum.Current;
end;

destructor TBaseEnum<T>.Destroy;
begin
  FEnum := nil;
  inherited;
end;

function TBaseEnum<T>.EOF: boolean;
begin
  result := FEnum.EOF;
end;

procedure TBaseEnum<T>.Next;
begin
  FEnum.Next;
end;

{ TFilterEnum<T> }

constructor TFilterEnum<T>.Create(AEnum: IEnum<T>; AFilter: IFilterFunction<T>);
begin
  inherited Create(AEnum);
  FFilter := AFilter;
  Next;
end;

constructor TFilterEnum<T>.Create(AEnum: IEnum<T>; AFilter: TFilterFunction<T>);
begin
  Create(AEnum, TTypedFunctionFilter<T>.Create(AFilter));
end;

function TFilterEnum<T>.Current: T;
begin
  result := FNext;
end;

destructor TFilterEnum<T>.Destroy;
begin
  FFilter := nil;
  inherited;
end;

function TFilterEnum<T>.EOF: boolean;
begin
  result := FEnum.EOF and not FHasValue;
end;

procedure TFilterEnum<T>.Next;
begin
  FHasValue := false;
  if FEnum.EOF then
    exit;
  while not FEnum.EOF do
  begin
    FNext := FEnum.Current;
    FEnum.Next;
    if FFilter.IsTrue(FNext) then
    begin
      FHasValue := true;
      break;
    end;
  end;
end;

{ FSkip<T> }

constructor TSkip<T>.Create(AEnum: IEnum<T>; ASkip: integer);
begin
  inherited Create(AEnum);
  while (ASkip > 0) and not EOF do
  begin
    Next;
    dec(ASkip);
  end;
end;

{ TTake<T> }

constructor TTake<T>.Create(AEnum: IEnum<T>; ATake: integer);
begin
  inherited Create(AEnum);
  FTake := ATake;
end;

function TTake<T>.EOF: boolean;
begin
  result := FEnum.EOF or FEof;
end;

procedure TTake<T>.Next;
begin
  FEnum.Next;
  if FTake > 0 then
    dec(FTake);
  if FTake = 0 then
    FEof := true;
end;

{ TMap<TInput, TOutput> }

constructor TMapEnum<TInput, TOutput>.Create(AEnum: IEnum<TInput>; AMapper: TMapFunction<TInput, TOutput>);
begin
  inherited Create();
  FEnum := AEnum;
  FMapper := AMapper;
end;

function TMapEnum<TInput, TOutput>.Current: TOutput;
begin
  result := FMapper(FEnum.Current)
end;

destructor TMapEnum<TInput, TOutput>.Destroy;
begin
  FEnum := nil;
  inherited;
end;

function TMapEnum<TInput, TOutput>.EOF: boolean;
begin
  result := FEnum.EOF;
end;

procedure TMapEnum<TInput, TOutput>.Next;
begin
  FEnum.Next;
end;

{ TEnumCache<T> }

constructor TEnumCache<T>.Create(AEnum: IEnum<T>; const AOwn: boolean);
begin
  inherited Create();
  FCache := TList<T>.Create;
  FOwn := AOwn;
  while AEnum.HasMore do
  begin
    FCache.Add(AEnum.Current);
  end;
end;

constructor TEnumCache<T>.Create(AEnum: TList<T>);
begin
  inherited Create();
  FCache := AEnum;
  FOwn := false;
end;

constructor TEnumCache<T>.Create(AEnum: TEnumerable<T>);
var
  e: TEnumerator<T>;
begin
  inherited Create();
  FCache := TList<T>.Create;
  FOwn := false;
  e := AEnum.GetEnumerator;
  while e.MoveNext do
  begin
    FCache.Add(e.Current);
  end;
end;

destructor TEnumCache<T>.Destroy;
var
  val: T;
  obj: TObject;
begin
  if FOwn then
  begin
    for val in FCache do
    begin
      move(val, obj, sizeof(obj));
      obj.Free;
    end;
  end;
  FCache.Free;
  inherited;
end;

function TEnumCache<T>.GetCache: TList<T>;
begin
  result := FCache;
end;

function TEnumCache<T>.GetEnum: IEnum<T>;
begin
  result := TCachedEnum<T>.Create(self);
end;

{ TApplyEnum<T> }

constructor TApplyEnum<T>.Create(AEnum: IEnum<T>; const AApply: TApplyFunction<T>);
begin
  inherited Create(AEnum);
  FApply := AApply;
end;

function TApplyEnum<T>.Current: T;
begin
  result := FEnum.Current;
  FApply(result);
end;

{ THasMore<T> }

constructor THasMore<T>.Create;
begin
  FFirst := false;
end;

function THasMore<T>.HasMore: boolean;
begin
  if FFirst then
    Next;
  FFirst := true;
  result := not EOF;
end;

{ Enum }

class function Enum.ToArray<T>(AEnum: IEnum<T>): TArray<T>;
begin
  setlength(result, 0);
  while AEnum.HasMore do
    insert(AEnum.Current, result, length(result));
end;

class function Enum.ToList<T>(AEnum: IEnum<T>): TList<T>;
begin
  result := TList<T>.Create;
  while AEnum.HasMore do
    result.Add(AEnum.Current);
end;

class function Enum.TryGetCached<T>(AEnum: IEnum<T>; out ACachedEnum: IEnum<T>): boolean;
var
  c: TCachedEnum<T>;
begin
  if Enum.IsCached<T>(AEnum) then
  begin
    c := AEnum as TCachedEnum<T>;
    ACachedEnum := c.GetEnum;
    exit(true);
  end;
  exit(false);
end;

class procedure Enum.Apply<T>(AEnum: IEnum<T>; const AFunc: TApplyFunction<T>);
var
  v: T;
begin
  while AEnum.HasMore do
  begin
    v := AEnum.Current;
    AFunc(v);
  end;
end;

class function Enum.Cache<T>(AEnum: IEnum<T>): IEnum<T>;
var
  res: IEnum<T>;
begin
  if Enum.TryGetCached<T>(AEnum, res) then
    exit(res);
  result := TEnumCache<T>.Create(AEnum).GetEnum;
end;

class function Enum.Cache<T>(AEnum: TList<T>): IEnum<T>;
begin
  result := TEnumCache<T>.Create(AEnum).GetEnum;
end;

class function Enum.Count<T>(AEnum: IEnum<T>): integer;
begin
  result := 0;
  while AEnum.HasMore do
    inc(result);
end;

class function Enum.GroupBy<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TValueType>;
var
  extractor: IFieldExtractor;
  valueT: T;
  val, keyVal: TValue;
begin
  result := TDictionary<TKeyType, TValueType>.Create();
  extractor := StreamCache.getextractor(rttictx.gettype(typeinfo(T)), AField.Field);
  while AEnum.HasMore do
  begin
    valueT := AEnum.Current;
    val := TValue.From<T>(valueT);
    extractor.GetValue(val, keyVal);
    result.AddOrSetValue(keyVal.AsType<TKeyType>(), AFunction(valueT));
  end;
end;

class function Enum.GroupBy<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, T>;
begin
  result := Enum.GroupBy<T, TKeyType, T>(AEnum, AField,
    function(const A: T): T
    begin
      result := A;
    end);
end;

class function Enum.GroupToArray<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TArray<TValueType>>;
var
  extractor: IFieldExtractor;
  valueT: T;
  val, keyVal: TValue;
  lst: TArray<TValueType>;
  key: TKeyType;
begin
  result := TDictionary < TKeyType, TArray < TValueType >>.Create();
  extractor := StreamCache.getextractor(rttictx.gettype(typeinfo(T)), AField.Field);
  while AEnum.HasMore do
  begin
    valueT := AEnum.Current;
    val := TValue.From<T>(valueT);
    if not extractor.GetValue(val, keyVal) then
      raise EStream.CreateFmt('Key ''%s'' not found', [AField.Field]);
    key := keyVal.AsType<TKeyType>();
    if not result.TryGetValue(key, lst) then
    begin
      lst := [AFunction(valueT)];
      result.Add(key, lst);
    end
    else
    begin
      lst := lst + [AFunction(valueT)];
      result.AddOrSetValue(key, lst);
    end;
  end;
end;

class function Enum.GroupToArray<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, TArray<T>>;
begin
  result := Enum.GroupToArray<T, TKeyType, T>(AEnum, AField,
    function(const A: T): T
    begin
      result := A;
    end);
end;

class function Enum.GroupToLists<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TList<TValueType>>;
var
  extractor: IFieldExtractor;
  valueT: T;
  val, keyVal: TValue;
  lst: TList<TValueType>;
  key: TKeyType;
begin
  result := TDictionary < TKeyType, TList < TValueType >>.Create();
  extractor := StreamCache.getextractor(rttictx.gettype(typeinfo(T)), AField.Field);
  while AEnum.HasMore do
  begin
    valueT := AEnum.Current;
    val := TValue.From<T>(valueT);
    if not extractor.GetValue(val, keyVal) then
      raise EStream.CreateFmt('Key ''%s'' not found', [AField.Field]);
    key := keyVal.AsType<TKeyType>();
    if not result.TryGetValue(key, lst) then
    begin
      lst := TList<TValueType>.Create;
      result.Add(key, lst);
    end;
    lst.Add(AFunction(valueT));
  end;
end;

class function Enum.GroupToLists<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, TList<T>>;
begin
  result := Enum.GroupToLists<T, TKeyType, T>(AEnum, AField,
    function(const A: T): T
    begin
      result := A;
    end);
end;

class function Enum.IsCached<T>(AEnum: IEnum<T>): boolean;
var
  o: TObject;
begin
  o := AEnum as TObject;
  result := AEnum is TCachedEnum<T>;
end;

class function Enum.Map<T, TOutput>(AEnum: IEnum<T>; const AFunction: TMapFunction<T, TOutput>): TArray<TOutput>;
begin
  setlength(result, 0);
  while AEnum.HasMore do
    insert(AFunction(AEnum.Current), result, length(result));
end;

class function Enum.Cache<T>(AEnum: TEnumerable<T>): IEnum<T>;
begin
  result := TEnumCache<T>.Create(AEnum).GetEnum;
end;

{ TCachedEnum<T> }

constructor TCachedEnum<T>.Create(Cache: IEnumCache<T>);
begin
  inherited Create();
  FCache := Cache;
  FEnum := TEnumerableEnum2<T>.Create(GetCache);
end;

function TCachedEnum<T>.Current: T;
begin
  result := FEnum.Current;
end;

destructor TCachedEnum<T>.Destroy;
begin
  FCache := nil;
  FEnum := nil;
  inherited;
end;

function TCachedEnum<T>.EOF: boolean;
begin
  result := FEnum.EOF;
end;

function TCachedEnum<T>.GetCache: TList<T>;
begin
  result := FCache.GetCache;
end;

function TCachedEnum<T>.GetEnum: IEnum<T>;
begin
  result := TCachedEnum<T>.Create(FCache);
end;

procedure TCachedEnum<T>.Next;
begin
  FEnum.Next;
end;

{ TEnumerableEnum2<T> }

constructor TEnumerableEnum2<T>.Create(const AEnum: TEnumerable<T>);
begin
  inherited Create();
  FEnum := AEnum.GetEnumerator;
  Next;
end;

function TEnumerableEnum2<T>.Current: T;
begin
  result := FEnum.Current;
end;

destructor TEnumerableEnum2<T>.Destroy;
begin
  FEnum.Free;
  inherited;
end;

function TEnumerableEnum2<T>.EOF: boolean;
begin
  result := FEof;
end;

procedure TEnumerableEnum2<T>.Next;
begin
  FEof := not FEnum.MoveNext;
end;

{ TSortedEnum<T> }

constructor TSortedEnum<T>.Create(Enum: IEnum<T>; comparator: IComparer<T>);
var
  v: T;
  i: integer;
begin
  inherited Create();
  FItems := TList<T>.Create;
  while Enum.HasMore do
  begin
    v := Enum.Current;
    FItems.BinarySearch(v, i, comparator);
    FItems.insert(i, v);
  end;
  FEnum := TEnumerableEnum2<T>.Create(FItems);
end;

function TSortedEnum<T>.Current: T;
begin
  result := FEnum.Current;
end;

destructor TSortedEnum<T>.Destroy;
begin
  FItems.Free;
  FEnum := nil;
  inherited;
end;

function TSortedEnum<T>.EOF: boolean;
begin
  result := FEnum.EOF;
end;

procedure TSortedEnum<T>.Next;
begin
  FEnum.Next;
end;

{ TJoinEnum<TLeft, TRight, TJoined> }

constructor TJoinEnum<TLeft, TRight, TJoined>.Create( //
  AEnumLeft: IEnum<TLeft>; AEnumRight: IEnum<TRight>; //
const AOn: TJoinOnFunction<TLeft, TRight>; const ASelect: TJoinSelectFunction<TLeft, TRight, TJoined>);
begin
  inherited Create();
  FEnumLeft := AEnumLeft;
  FEnumRight := Enum.Cache<TRight>(AEnumRight);
  FOn := AOn;
  FSelect := ASelect;
  FHasLeft := FEnumLeft.HasMore;
  FHasRight := FEnumRight.HasMore;
  FindNext;
end;

function TJoinEnum<TLeft, TRight, TJoined>.Current: TJoined;
begin
  result := FNext;
end;

destructor TJoinEnum<TLeft, TRight, TJoined>.Destroy;
begin
  FEnumLeft := nil;
  FEnumRight := nil;
  inherited;
end;

function TJoinEnum<TLeft, TRight, TJoined>.EOF: boolean;
begin
  result := not FHasNext;
end;

procedure TJoinEnum<TLeft, TRight, TJoined>.FindNext;
var
  left: TLeft;
  right: TRight;
  match: boolean;
begin
  FHasNext := false;
  while FHasLeft and FHasRight do
  begin
    left := FEnumLeft.Current;
    right := FEnumRight.Current;

    match := FOn(left, right);
    if match then
    begin
      FHasNext := true;
      FNext := FSelect(left, right);
    end;

    FHasRight := FEnumRight.HasMore;
    if not FHasRight then
    begin
      FHasLeft := FEnumLeft.HasMore;
      ResetRight;
    end;
    if match then
      exit;
  end;
end;

procedure TJoinEnum<TLeft, TRight, TJoined>.Next;
begin
  FindNext;
end;

procedure TJoinEnum<TLeft, TRight, TJoined>.ResetRight;
var
  res: IEnum<TRight>;
begin
  if Enum.TryGetCached<TRight>(FEnumRight, res) then
    FEnumRight := res;
  FHasRight := FEnumRight.HasMore;
end;

{ TUnionEnum<T> }

constructor TUnionEnum<T>.Create(Enum: TArray < IEnum < T >> );
begin
  inherited Create();
  FEnums := Enum;
  FIdx := 0;
end;

function TUnionEnum<T>.Current: T;
begin
  result := FEnums[FIdx].Current;
end;

destructor TUnionEnum<T>.Destroy;
begin
  FEnums := nil;
  inherited;
end;

function TUnionEnum<T>.EOF: boolean;
begin
  if (length(FEnums) = 0) or (FIdx >= length(FEnums)) then
    exit(true);
  result := FEnums[FIdx].EOF;
end;

procedure TUnionEnum<T>.Next;
begin
  FEnums[FIdx].Next;
  if EOF then
    inc(FIdx);
end;

{ TLeftJoinEnum<TLeft, TRight, TJoined> }

constructor TLeftJoinEnum<TLeft, TRight, TJoined>.Create(AEnumLeft: IEnum<TLeft>; AEnumRight: IEnum<TRight>; const AOn: TJoinOnFunction<TLeft, TRight>;
const ASelect: TJoinSelectFunction<TLeft, TRight, TJoined>);
begin
  inherited Create();
  FEnumLeft := AEnumLeft;
  // if not cached, we cache it
  // if it is cached, we get a new enum
  if not Enum.TryGetCached<TRight>(AEnumRight, FEnumRight) then
    FEnumRight := Enum.Cache<TRight>(AEnumRight);
  FOn := AOn;
  FSelect := ASelect;
  FHasLeft := FEnumLeft.HasMore;
  FHasRight := FEnumRight.HasMore;
  FFoundRight := false;
  FindNext;

end;

function TLeftJoinEnum<TLeft, TRight, TJoined>.Current: TJoined;
begin
  result := FNext;
end;

destructor TLeftJoinEnum<TLeft, TRight, TJoined>.Destroy;
begin
  FEnumLeft := nil;
  FEnumRight := nil;
  inherited;
end;

function TLeftJoinEnum<TLeft, TRight, TJoined>.EOF: boolean;
begin
  result := not FHasNext;
end;

procedure TLeftJoinEnum<TLeft, TRight, TJoined>.FindNext;
var
  left: TLeft;
  right: TRight;
  match: boolean;
begin
  FHasNext := false;
  while FHasLeft and FHasRight do
  begin
    left := FEnumLeft.Current;
    right := FEnumRight.Current;
    match := FOn(left, right);
    if match then
    begin
      FHasNext := true;
      FFoundRight := true;
      FNext := FSelect(left, right);
    end;
    FHasRight := FEnumRight.HasMore;
    if not FHasRight then
    begin
      if not FFoundRight then
      begin
        fillchar(right, sizeof(right), 0);
        FHasNext := true;
        FNext := FSelect(left, right);
        match := true;
      end;

      FHasLeft := FEnumLeft.HasMore;
      ResetRight;
    end;
    if match then
      exit;
  end;
end;

procedure TLeftJoinEnum<TLeft, TRight, TJoined>.Next;
begin
  FindNext;
end;

procedure TLeftJoinEnum<TLeft, TRight, TJoined>.ResetRight;
var
  res: IEnum<TRight>;
begin
  if Enum.TryGetCached<TRight>(FEnumRight, res) then
    FEnumRight := res;
  FFoundRight := false;
  FHasRight := FEnumRight.HasMore;
end;

end.
