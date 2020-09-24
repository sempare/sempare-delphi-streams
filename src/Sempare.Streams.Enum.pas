unit Sempare.Streams.Enum;

interface

uses
  System.Generics.Defaults,
  System.Generics.Collections,
  Sempare.Streams.Types,
  Sempare.Streams.Filter;

type
  IEnum<T> = interface
    ['{B5EAE436-8EE0-404A-B842-E6BD90B23E6F}']
    function EOF: boolean;
    procedure Next;
    function Current: T;
    function HasMore: boolean;
  end;

  Enum = class
  public
    class function Count<T>(AEnum: IEnum<T>): integer; static;
    class function ToArray<T>(AEnum: IEnum<T>): TArray<T>; static;
    class function ToList<T>(AEnum: IEnum<T>): TList<T>; static;
    class procedure Apply<T>(AEnum: IEnum<T>; const AFunc: TApplyFunction<T>); static;
    // class function Cache<T>(AEnum: IEnum<T>): IEnum<T>; static;
    class function Map<T, TOutput>(AEnum: IEnum<T>; const AFunction: TMapFunction<T, TOutput>): TArray<TOutput>; static;
  end;

  THasMore<T> = class abstract(TInterfacedObject, IEnum<T>)
  private
    FFirst: boolean;
  public
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
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

  TBaseEnum<T> = class abstract(THasMore<T>)
  protected
    FEnum: IEnum<T>;
  public
    constructor Create(AEnum: IEnum<T>);
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

  (* ICache<T> = interface
    ['{704AB8CE-4AD0-4166-A235-F0B2F8C0A20D}']
    function GetEnum: IEnum<T>;
    end;

    TCache<T> = class(TInterfacedObject, ICache<T>)
    private
    FCache: TList<T>;
    FOwn: boolean;
    public
    constructor Create(AEnum: IEnum<T>; const AOwn: boolean = false);
    destructor Destroy; override;
    function GetEnum: IEnum<T>;
    end;

    TCachedEnum<T> = class(THasMore<T>, ICache<T>)
    private
    FCache: ICache<T>;
    FEnum: IEnum<T>;
    public
    constructor Create(AEnum: IEnum<T>; const AOwn: boolean = false);
    destructor Destroy; override;
    function EOF: boolean; override;
    procedure Next; override;
    function Current: T; override;
    function GetEnum: IEnum<T>;
    end; *)

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

implementation

uses
  System.SysUtils,
  System.Rtti;

{ TArrayEnum<T> }

constructor TArrayEnum<T>.Create(const AData: TArray<T>);
begin
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
  FEnum := AEnum.GetEnumerator;
  Next;
end;

function TEnumerableEnum<T>.Current: T;
begin
  result := FEnum.Current;
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
  FEnum := AEnum;
end;

function TBaseEnum<T>.Current: T;
begin
  result := FEnum.Current;
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
(*
  { TCache<T> }

  constructor TCache<T>.Create(AEnum: IEnum<T>; const AOwn: boolean);
  begin
  FCache := TList<T>.Create;
  FOwn := AOwn;
  end;

  destructor TCache<T>.Destroy;
  var
  val: T;
  obj: TObject;
  begin
  if FOwn then
  for val in FCache do
  begin
  move(val, obj, sizeof(obj));
  obj.Free;
  end;
  FCache.Free;
  inherited;
  end;

  function TCache<T>.GetEnum: IEnum<T>;
  begin
  result := TEnumerableEnum<T>.Create(FCache);
  end; *)

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
(*
  class function Enum.Cache<T>(AEnum: IEnum<T>): IEnum<T>;
  var
  c: ICache<T>;
  begin
  if AEnum is TCachedEnum<T> then
  begin
  if supports(AEnum, ICache<T>, c) then
  exit(c.GetEnum);
  end;
  exit(nil);
  end; *)

class function Enum.Count<T>(AEnum: IEnum<T>): integer;
begin
  result := 0;
  while AEnum.HasMore do
    inc(result);
end;

class function Enum.Map<T, TOutput>(AEnum: IEnum<T>; const AFunction: TMapFunction<T, TOutput>): TArray<TOutput>;
begin
  setlength(result, 0);
  while AEnum.HasMore do
    insert(AFunction(AEnum.Current), result, length(result));
end;
(*
  { TCachedEnum<T> }

  constructor TCachedEnum<T>.Create(AEnum: IEnum<T>; const AOwn: boolean);
  begin
  FCache := TCache<T>.Create(AEnum, AOwn);
  FEnum := GetEnum;
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

  function TCachedEnum<T>.GetEnum: IEnum<T>;
  begin
  result := FCache.GetEnum;
  end;

  procedure TCachedEnum<T>.Next;
  begin
  FEnum.Next;
  end; *)

{ TEnumerableEnum2<T> }

constructor TEnumerableEnum2<T>.Create(const AEnum: TEnumerable<T>);
begin
  FEnum := AEnum.GetEnumerator;
  Next;
end;

function TEnumerableEnum2<T>.Current: T;
begin
  result := FEnum.Current;
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

end.
