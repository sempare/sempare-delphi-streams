(*%****************************************************************************
 *                 ___                                                        *
 *                / __|  ___   _ __    _ __   __ _   _ _   ___                *
 *                \__ \ / -_) | '  \  | '_ \ / _` | | '_| / -_)               *
 *                |___/ \___| |_|_|_| | .__/ \__,_| |_|   \___|               *
 *                                    |_|                                     *
 ******************************************************************************
 *                                                                            *
 *                        Sempare Streams                                     *
 *                                                                            *
 *                                                                            *
 *          https://www.github.com/sempare/sempare-streams                    *
 ******************************************************************************
 *                                                                            *
 * Copyright (c) 2020 Sempare Limited,                                        *
 *                    Conrad Vermeulen <conrad.vermeulen@gmail.com>           *
 *                                                                            *
 * Contact: info@sempare.ltd                                                  *
 *                                                                            *
 * Licensed under the GPL Version 3.0 or the Sempare Commercial License       *
 * You may not use this file except in compliance with one of these Licenses. *
 * You may obtain a copy of the Licenses at                                   *
 *                                                                            *
 * https://www.gnu.org/licenses/gpl-3.0.en.html                               *
 * https://github.com/sempare/sempare-streams/tree/dev/docs/commercial.license.md *
 *                                                                            *
 * Unless required by applicable law or agreed to in writing, software        *
 * distributed under the Licenses is distributed on an "AS IS" BASIS,          *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 * See the License for the specific language governing permissions and        *
 * limitations under the License.                                             *
 *                                                                            *
 ****************************************************************************%*)
unit Sempare.Streams.Enum;

interface

{$I 'Sempare.Streams.inc'}

uses
  Data.DB,
{$IF defined(SEMPARE_STREAMS_SPRING4D_SUPPORT)}
  Spring.Collections,
{$ENDIF}
  System.Rtti,
  System.Generics.Defaults,
  System.Generics.Collections,
  Sempare.Streams.Types,
  Sempare.Streams.Filter;

type
  /// <summary>
  /// Enum is a utility class for enumerable operations
  /// <summary>
  Enum = class
  public
    class function AreEqual<T>(AEnumA, AEnumB: IEnum<T>): boolean; overload; static;
    class function AreEqual<T>(AEnumA, AEnumB: IEnum<T>; comparator: IComparer<T>): boolean; overload; static;
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

  /// <summary>
  /// THasMore is a base class for all enumerable classes. There are two ways the instances
  /// can be used.
  /// <code>
  /// while enum.HasMore do
  /// doSomething(enum.Current);
  /// </code>
  /// or
  /// <code>
  /// while not enum.EOF do
  /// begin
  /// doSomething(enum.Current);
  /// enum.Next;
  /// end;
  /// </code>
  /// <summary>
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

  /// <summary>
  /// TArrayEnum is an enumerator over an dynarray (TArray)
  /// </summary>
  TArrayEnum<T> = class(THasMore<T>, IEnumCache<T>)
  private
    FData: TArray<T>;
    FOffset: integer;
  public
    constructor Create(const AData: TArray<T>);
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
    function GetEnum: IEnum<T>;
    function GetCache: TList<T>;
  end;

  /// <summary>
  /// TTEnumerableEnum is an enumerator over a TEnumerable
  /// </summary>
  TTEnumerableEnum<T> = class(THasMore<T>)
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

  /// <summary>
  /// TIEnumerableEnum is an enumerator over an IEnumerable
  /// </summary>
  TIEnumerableEnum<T> = class(THasMore<T>)
  private
    FEnum: System.IEnumerator<T>;
    FEof: boolean;
  public
    constructor Create(const AEnum: System.IEnumerable<T>);
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

{$IF defined(SEMPARE_STREAMS_SPRING4D_SUPPORT)}

  /// <summary>
  /// TSpringIEnumerableEnum is an enumerator over a Spring4d IEnumerable
  /// </summary>
  TSpringIEnumerableEnum<T> = class(THasMore<T>)
  private
    FEnum: Spring.Collections.IEnumerator<T>;
    FEof: boolean;
  public
    constructor Create(const AEnum: Spring.Collections.IEnumerable<T>);
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;
{$ENDIF}

  /// <summary>
  /// TBaseEnum is a wrapper around another enumerator
  /// </summary>
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

  /// <summary>
  /// TSortedEnum ensures items are sorted.
  /// </summary>
  TSortedEnum<T> = class(TBaseEnum<T>, IEnumCache<T>)
  private
    FItems: TList<T>;
  public
    constructor Create(Enum: IEnum<T>; comparator: IComparer<T>);
    destructor Destroy; override;
    function GetEnum: IEnum<T>;
    function GetCache: TList<T>;
  end;

  /// <summary>
  /// TUniqueEnum ensures items are unique.
  /// </summary>
  TUniqueEnum<T> = class(TBaseEnum<T>, IEnumCache<T>)
  private
    FItems: TList<T>;
  public
    constructor Create(Enum: IEnum<T>; comparator: IComparer<T>);
    destructor Destroy; override;
    function GetEnum: IEnum<T>;
    function GetCache: TList<T>;
  end;

  /// <summary>
  /// TUnionEnum allows multiple enums be seen as a single stream.
  /// </summary>
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

  /// <summary>
  /// TIntRange allows us to enumerate a range of ints
  /// </summary>
  TIntRangeEnum = class(THasMore<Int64>)
  private
    FIdx: Int64;
    FEnd, FDelta: Int64;
  public
    constructor Create(const AStart, AEnd: Int64; const ADelta: Int64 = 1);
    function EOF: boolean; override;
    function Current: Int64; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TFloatRange allows us to enumerate a range of ints
  /// </summary>
  TFloatRangeEnum = class(THasMore<extended>)
  private
    FIdx: extended;
    FEnd, FDelta: extended;
  public
    constructor Create(const AStart, AEnd: extended; const ADelta: extended = 1);
    function EOF: boolean; override;
    function Current: extended; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TStringEnum allows us to enumerate a range of ints
  /// </summary>
  TStringEnum = class(THasMore<char>)
  private
    FValue: string;
    FIdx, FEnd: Int64;
  public
    constructor Create(const AValue: string);
    function EOF: boolean; override;
    function Current: char; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TFilterEnum only returns items that for which the filter functions returns true.
  /// </summary>
  TFilterEnum<T> = class(TBaseEnum<T>)
  private
    FFilter: IFilterFunction<T>;
    FNext: T;
    FHasValue: boolean;
  public
    constructor Create(AEnum: IEnum<T>; const AFilter: TFilterFunction<T>); overload;
    constructor Create(AEnum: IEnum<T>; const AFilter: IFilterFunction<T>); overload;
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TSkip skips the first few items.
  /// </summary>
  TSkip<T> = class(TBaseEnum<T>)
  public
    constructor Create(AEnum: IEnum<T>; const ASkip: integer);
  end;

  /// <summary>
  /// TTake identifies how many items should be returned.
  /// </summary>
  TTake<T> = class(TBaseEnum<T>)
  private
    FTake: integer;
    FEof: boolean;
  public
    constructor Create(AEnum: IEnum<T>; const ATake: integer);
    function EOF: boolean; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TApplyEnum applies a procedure to each item in a stream
  /// </summary>
  TApplyEnum<T> = class(TBaseEnum<T>)
  private
    FApply: TApplyFunction<T>;
  public
    constructor Create(AEnum: IEnum<T>; const AApply: TApplyFunction<T>);
    function Current: T; override;
  end;

  /// <summary>
  /// TEnumCache contains an array of items. GetEnum returns a TCachedEnum
  /// </summary>
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

  /// <summary>
  /// TCachedEnum enumerates items in the cache
  /// </summary>
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

  /// <summary>
  /// TMapEnum applies a function to items in the stream.
  /// </summary>
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

  /// <summary>
  /// TJoinEnum allows for an 'inner join' to be done between two streams.
  /// A matching function is required as well as a function that joins matching results.
  /// </summary>
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

  /// <summary>
  /// TLeftJoinEnum allows for an 'left join' to be done between two streams.
  /// A matching function is required as well as a function that joins matching results.
  /// </summary>
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

  /// <summary>
  /// TDataSetEnum enumerates a dataset with results into a managed type of T.
  /// </summary>
  TDataSetEnumRecord<T> = class(THasMore<T>)
  private
    FDataSet: TDataSet;
    FFields: TDictionary<string, string>;
    FRttiType: TRttiType;
  public
    constructor Create(const ADataSet: TDataSet);
    destructor Destroy; override;
    function Current: T; override;
    function EOF: boolean; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TDataSetEnum enumerates a dataset with results into a class of type T.
  /// </summary>
  TDataSetEnumClass<T> = class(THasMore<T>)
  private
    FDataSet: TDataSet;
    FConstructor: TRttiMethod;
    FFields: TDictionary<string, string>;
    FRttiType: TRttiType;
  public
    constructor Create(const ADataSet: TDataSet);
    destructor Destroy; override;
    function Current: T; override;
    function EOF: boolean; override;
    procedure Next; override;
  end;

implementation

uses
  System.SysUtils,

  Sempare.Streams,
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

function TArrayEnum<T>.GetCache: TList<T>;
begin
  result := Stream.From<T>(FData).ToList;
end;

function TArrayEnum<T>.GetEnum: IEnum<T>;
begin
  result := TArrayEnum<T>.Create(FData);
end;

procedure TArrayEnum<T>.Next;
begin
  if EOF then
    exit;
  inc(FOffset);
end;

{ TIEnumerableEnum<T> }

constructor TIEnumerableEnum<T>.Create(const AEnum: System.IEnumerable<T>);
begin
  inherited Create();
  FEnum := AEnum.GetEnumerator;
  Next;
end;

function TIEnumerableEnum<T>.Current: T;
begin
  result := FEnum.Current;
end;

destructor TIEnumerableEnum<T>.Destroy;
begin
  FEnum := nil;
  inherited;
end;

function TIEnumerableEnum<T>.EOF: boolean;
begin
  result := FEof;
end;

procedure TIEnumerableEnum<T>.Next;
begin
  FEof := not FEnum.MoveNext;
end;

{$IF defined(SEMPARE_STREAMS_SPRING4D_SUPPORT)}
{ TSpringIEnumerableEnum<T> }

constructor TSpringIEnumerableEnum<T>.Create(const AEnum: Spring.Collections.IEnumerable<T>);
begin
  inherited Create();
  FEnum := AEnum.GetEnumerator;
  Next;
end;

function TSpringIEnumerableEnum<T>.Current: T;
begin
  result := FEnum.Current;
end;

destructor TSpringIEnumerableEnum<T>.Destroy;
begin
  FEnum := nil;
  inherited;
end;

function TSpringIEnumerableEnum<T>.EOF: boolean;
begin
  result := FEof;
end;

procedure TSpringIEnumerableEnum<T>.Next;
begin
  FEof := not FEnum.MoveNext;
end;
{$ENDIF}

{ TBaseEnum<T> }

constructor TBaseEnum<T>.Create(AEnum: IEnum<T>);
begin
  inherited Create();
  if not Enum.TryGetCached<T>(AEnum, FEnum) then
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

constructor TFilterEnum<T>.Create(AEnum: IEnum<T>; const AFilter: IFilterFunction<T>);
begin
  inherited Create(AEnum);
  FFilter := AFilter;
  Next;
end;

constructor TFilterEnum<T>.Create(AEnum: IEnum<T>; const AFilter: TFilterFunction<T>);
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

constructor TSkip<T>.Create(AEnum: IEnum<T>; const ASkip: integer);
var
  skip: integer;
begin
  inherited Create(AEnum);
  skip := ASkip;
  while (skip > 0) and not EOF do
  begin
    Next;
    dec(skip);
  end;
end;

{ TTake<T> }

constructor TTake<T>.Create(AEnum: IEnum<T>; const ATake: integer);
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
  if not Enum.TryGetCached<TInput>(AEnum, FEnum) then
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
var
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  setlength(result, 0);
  while e.HasMore do
    insert(e.Current, result, length(result));
end;

class function Enum.ToList<T>(AEnum: IEnum<T>): TList<T>;
var
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := TList<T>.Create;
  while e.HasMore do
    result.Add(e.Current);
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
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  while e.HasMore do
  begin
    v := e.Current;
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
var
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := 0;
  while e.HasMore do
    inc(result);
end;

class function Enum.GroupBy<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TValueType>;
var
  extractor: IFieldExtractor;
  valueT: T;
  val, keyVal: TValue;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := TDictionary<TKeyType, TValueType>.Create();
  extractor := StreamCache.getextractor(rttictx.gettype(typeinfo(T)), AField.Field);
  while e.HasMore do
  begin
    valueT := e.Current;
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
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := TDictionary < TKeyType, TArray < TValueType >>.Create();
  extractor := StreamCache.getextractor(rttictx.gettype(typeinfo(T)), AField.Field);
  while e.HasMore do
  begin
    valueT := e.Current;
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
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := TDictionary < TKeyType, TList < TValueType >>.Create();
  extractor := StreamCache.getextractor(rttictx.gettype(typeinfo(T)), AField.Field);
  while e.HasMore do
  begin
    valueT := e.Current;
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
var
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  setlength(result, 0);
  while e.HasMore do
    insert(AFunction(e.Current), result, length(result));
end;

class function Enum.AreEqual<T>(AEnumA, AEnumB: IEnum<T>): boolean;
begin
  result := Enum.AreEqual<T>(AEnumA, AEnumB, TComparer<T>.default);
end;

class function Enum.AreEqual<T>(AEnumA, AEnumB: IEnum<T>; comparator: IComparer<T>): boolean;
var
  A, b: T;
  amore, bmore: boolean;
begin
  while true do
  begin
    amore := AEnumA.HasMore;
    bmore := AEnumB.HasMore;
    if not amore and not bmore then
      exit(true);

    if amore <> bmore then
      exit(false);

    if comparator.Compare(AEnumA.Current, AEnumB.Current) <> 0 then
      exit(false);
  end;
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
  FEnum := TTEnumerableEnum<T>.Create(GetCache);
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

constructor TTEnumerableEnum<T>.Create(const AEnum: TEnumerable<T>);
begin
  inherited Create();
  FEnum := AEnum.GetEnumerator;
  Next;
end;

function TTEnumerableEnum<T>.Current: T;
begin
  result := FEnum.Current;
end;

destructor TTEnumerableEnum<T>.Destroy;
begin
  FEnum.Free;
  inherited;
end;

function TTEnumerableEnum<T>.EOF: boolean;
begin
  result := FEof;
end;

procedure TTEnumerableEnum<T>.Next;
begin
  FEof := not FEnum.MoveNext;
end;

{ TSortedEnum<T> }

constructor TSortedEnum<T>.Create(Enum: IEnum<T>; comparator: IComparer<T>);
var
  v: T;
  i: integer;
begin
  FItems := TList<T>.Create;
  while Enum.HasMore do
  begin
    v := Enum.Current;
    FItems.BinarySearch(v, i, comparator);
    FItems.insert(i, v);
  end;
  inherited Create(TTEnumerableEnum<T>.Create(FItems));
end;

destructor TSortedEnum<T>.Destroy;
begin
  FItems.Free;
  inherited;
end;

function TSortedEnum<T>.GetCache: TList<T>;
begin
  result := FItems;
end;

function TSortedEnum<T>.GetEnum: IEnum<T>;
begin
  result := TTEnumerableEnum<T>.Create(FItems);
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

{ TUniqueEnum<T> }

constructor TUniqueEnum<T>.Create(Enum: IEnum<T>; comparator: IComparer<T>);
var
  v: T;
  i: integer;
begin
  FItems := TList<T>.Create;
  while Enum.HasMore do
  begin
    v := Enum.Current;
    if not FItems.BinarySearch(v, i, comparator) then
      FItems.insert(i, v);
  end;
  inherited Create(TTEnumerableEnum<T>.Create(FItems));
end;

destructor TUniqueEnum<T>.Destroy;
begin
  FItems.Free;
  inherited;
end;

function TUniqueEnum<T>.GetCache: TList<T>;
begin
  result := FItems;
end;

function TUniqueEnum<T>.GetEnum: IEnum<T>;
begin
  result := TTEnumerableEnum<T>.Create(FItems);
end;

{ TDataSetEnumClass<T> }

constructor TDataSetEnumClass<T>.Create(const ADataSet: TDataSet);
var
  attrib: TCustomAttribute;
  Field: trttifield;
  fieldname: string;
begin
  FDataSet := ADataSet;
  FDataSet.First;
  FFields := TDictionary<string, string>.Create;

  FRttiType := rttictx.gettype(typeinfo(T));
  FConstructor := FRttiType.GetMethod('Create');
  for Field in FRttiType.GetFields do
  begin
    fieldname := Field.Name;
    for attrib in Field.GetAttributes do
    begin
      if attrib is StreamFieldAttribute then
      begin
        fieldname := StreamFieldAttribute(attrib).Name;
        break;
      end;
    end;
    FFields.AddOrSetValue(Field.Name, fieldname);
  end;

end;

function TDataSetEnumClass<T>.Current: T;
var
  Field: trttifield;
  obj: TObject;
begin
  obj := FConstructor.Invoke(FRttiType.AsInstance.MetaclassType, []).AsObject;
  move(obj, result, sizeof(T)); // this is a hack
  for Field in FRttiType.GetFields do
  begin
    Field.SetValue(obj, TValue.fromVariant(FDataSet.FieldByName(FFields[Field.Name]).AsVariant));
  end;
end;

destructor TDataSetEnumClass<T>.Destroy;
begin
  FFields.Free;
  inherited;
end;

function TDataSetEnumClass<T>.EOF: boolean;
begin
  result := FDataSet.EOF;
end;

procedure TDataSetEnumClass<T>.Next;
begin
  FDataSet.Next;
end;

{ TDataSetEnumRecord<T> }

constructor TDataSetEnumRecord<T>.Create(const ADataSet: TDataSet);
var
  attrib: TCustomAttribute;
  Field: trttifield;
  fieldname: string;
begin
  FDataSet := ADataSet;
  FDataSet.First;
  FFields := TDictionary<string, string>.Create;

  FRttiType := rttictx.gettype(typeinfo(T));
  for Field in FRttiType.GetFields do
  begin
    fieldname := Field.Name;
    for attrib in Field.GetAttributes do
    begin
      if attrib is StreamFieldAttribute then
      begin
        fieldname := StreamFieldAttribute(attrib).Name;
        break;
      end;
    end;
    FFields.AddOrSetValue(Field.Name, fieldname);
  end;

end;

function TDataSetEnumRecord<T>.Current: T;
var
  Field: trttifield;

begin
  fillchar(result, sizeof(T), 0);
  for Field in FRttiType.GetFields do
  begin
    Field.SetValue(@result, TValue.fromVariant(FDataSet.FieldByName(FFields[Field.Name]).AsVariant));
  end;
end;

destructor TDataSetEnumRecord<T>.Destroy;
begin
  FFields.Free;
  inherited;
end;

function TDataSetEnumRecord<T>.EOF: boolean;
begin
  result := FDataSet.EOF;
end;

procedure TDataSetEnumRecord<T>.Next;
begin
  FDataSet.Next;
end;

{ TIntRange }

constructor TIntRangeEnum.Create(const AStart, AEnd: Int64; const ADelta: Int64);
begin
  inherited Create();
  FIdx := AStart;
  FEnd := AEnd;
  FDelta := ADelta;
end;

function TIntRangeEnum.Current: Int64;
begin
  result := FIdx;
end;

function TIntRangeEnum.EOF: boolean;
begin
  result := FIdx > FEnd;
end;

procedure TIntRangeEnum.Next;
begin
  inc(FIdx);
end;

{ TFloatRange }

constructor TFloatRangeEnum.Create(const AStart, AEnd, ADelta: extended);
begin
  FIdx := AStart;
  FEnd := AEnd;
  FDelta := ADelta;
end;

function TFloatRangeEnum.Current: extended;
begin
  result := FIdx;
end;

function TFloatRangeEnum.EOF: boolean;
begin
  result := FIdx > FEnd;
end;

procedure TFloatRangeEnum.Next;
begin
  FIdx := FIdx + FDelta;
end;

{ TStringEnum }

constructor TStringEnum.Create(const AValue: string);
begin
  FValue := AValue;
  FIdx := 1;
  FEnd := length(AValue);
end;

function TStringEnum.Current: char;
begin
  result := FValue[FIdx];
end;

function TStringEnum.EOF: boolean;
begin
  result := FIdx > FEnd;
end;

procedure TStringEnum.Next;
begin
  inc(FIdx);
end;

end.
