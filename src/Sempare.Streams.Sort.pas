unit Sempare.Streams.Sort;

interface

uses
  System.Generics.Collections,
  System.Generics.Defaults,
  System.Rtti,
  Sempare.Streams.Rtti,
  Sempare.Streams.Types;

type
  ISortExpr = interface
    ['{C16D3780-E9A6-412C-A589-958C8610AF3B}']
    function GetField: TRttiField;
    function GetName: string;
    function GetOrder: TSortOrder;
    procedure SetField(const Value: TRttiField);

    property Name: string read GetName;
    property Order: TSortOrder read GetOrder;
    property RttiField: TRttiField read GetField write SetField;
  end;

  TSortExpr = class(TInterfacedObject, ISortExpr)
  strict private
    FName: string;
    FOrder: TSortOrder;
    FField: TRttiField;
  private
    function GetField: TRttiField;
    function GetName: string;
    function GetOrder: TSortOrder;
    procedure SetField(const Value: TRttiField);
  public
    constructor Create(const AName: string; const AOrder: TSortOrder);
    property Name: string read GetName;
    property Order: TSortOrder read GetOrder;
    property RttiField: TRttiField read GetField write SetField;
  end;

  TSortFieldBase = class abstract(TInterfacedObject, IComparer<TValue>)
  public
    function Compare(const A, B: TValue): integer; virtual; abstract;
  end;

  TSortFieldString = class(TSortFieldBase)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TSortFieldInteger = class(TSortFieldBase)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TSortFieldDouble = class(TSortFieldBase)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TSortFieldBoolean = class(TSortFieldBase)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TSortFieldComposite<T> = class abstract(TInterfacedObject, IComparer<T>)
  strict protected
    FComparators: TArray<IComparer<TValue>>;
    FExtractors: TArray<IFieldExtractor>;
    FExprs: TArray<ISortExpr>;
  public
    constructor Create(AExprs: TArray<ISortExpr>); overload;
    constructor Create(AComparators: TArray<IComparer<TValue>>; AExprs: TArray<ISortExpr>; AExtractors: TArray<IFieldExtractor>); overload;
    destructor Destroy; override;
    function Compare(const A, B: T): integer;
  end;

var
  SortString: IComparer<TValue>;
  SortInt64: IComparer<TValue>;
  SortDouble: IComparer<TValue>;
  SortBoolean: IComparer<TValue>;

implementation

uses
  System.SysUtils;

{ TSortExpr }

constructor TSortExpr.Create(const AName: string; const AOrder: TSortOrder);
begin
  FName := AName;
  FOrder := AOrder;
end;

function TSortExpr.GetField: TRttiField;
begin
  result := FField;
end;

function TSortExpr.GetName: string;
begin
  result := FName;
end;

function TSortExpr.GetOrder: TSortOrder;
begin
  result := FOrder;
end;

procedure TSortExpr.SetField(const Value: TRttiField);
begin
  FField := Value;
end;

{ TSortFieldString }

function TSortFieldString.Compare(const A, B: TValue): integer;
var
  avs, bvs: string;
begin
  avs := A.asstring;
  bvs := B.asstring;
  if avs < bvs then
    result := -1
  else if avs = bvs then
    result := 0
  else
    result := 1;
end;

{ TSortFieldInteger }

function TSortFieldInteger.Compare(const A, B: TValue): integer;
var
  avs, bvs: int64;
begin
  avs := A.AsInt64;
  bvs := B.AsInt64;
  if avs < bvs then
    result := -1
  else if avs = bvs then
    result := 0
  else
    result := 1;
end;

{ TSortFieldDouble }

function TSortFieldDouble.Compare(const A, B: TValue): integer;
var
  avs, bvs: double;
begin
  avs := A.AsExtended;
  bvs := B.AsExtended;
  if avs < bvs then
    result := -1
  else if avs = bvs then
    result := 0
  else
    result := 1;
end;

{ TSortFieldBoolean }

function TSortFieldBoolean.Compare(const A, B: TValue): integer;
var
  avs, bvs: boolean;
begin
  avs := A.AsBoolean;
  bvs := B.AsBoolean;
  if avs < bvs then
    result := -1
  else if avs = bvs then
    result := 0
  else
    result := 1;
end;

{ TSortFieldComposite<T> }

function TSortFieldComposite<T>.Compare(const A, B: T): integer;
var
  i, cv: integer;
  av, bv: TValue;
  e: IFieldExtractor;
begin
  for i := 0 to length(FExtractors) - 1 do
  begin
    e := FExtractors[i];
    if not e.getvalue(TValue.From<T>(A), av) then
      exit(1);
    if not e.getvalue(TValue.From<T>(B), bv) then
      exit(-1);
    cv := FComparators[i].Compare(av, bv);
    if cv = 0 then
      continue;
    if FExprs[i].Order = soDescending then
      cv := -cv;
    exit(cv);
  end;
  result := 0;
end;

constructor TSortFieldComposite<T>.Create(AComparators: TArray<IComparer<TValue>>; AExprs: TArray<ISortExpr>; AExtractors: TArray<IFieldExtractor>);
begin
  FComparators := AComparators;
  FExtractors := AExtractors;
  FExprs := AExprs;
end;

constructor TSortFieldComposite<T>.Create(AExprs: TArray<ISortExpr>);
var
  RttiType: TRttiType;
  Comparer: IComparer<TValue>;
  comparators: TArray<IComparer<TValue>>;
  extractors: TArray<IFieldExtractor>;
  e: IFieldExtractor;
  Field: ISortExpr;
  i: integer;
begin
  RttiType := RttiCtx.GetType(typeinfo(T));
  setlength(comparators, 0);
  setlength(extractors, 0);
  for i := 0 to length(AExprs) - 1 do
  begin
    Field := AExprs[i];
    e := Streamcache.GetExtractor(RttiType, Field.Name);
    insert(e, extractors, length(extractors));
    case e.RttiType.TypeKind of
      tkInteger, tkInt64:
        Comparer := SortInt64;
      tkEnumeration:
        Comparer := SortBoolean;
      tkString, tkAnsiString, tkWideString, tkUnicodeString:
        Comparer := SortString;
      tkFloat:
        Comparer := SortDouble;
    else
      raise Exception.Create('type not supported');
    end;
    insert(Comparer, comparators, length(comparators));
  end;
  if length(AExprs) = 0 then
    raise Exception.Create('sort expressions expected');

  Create(comparators, AExprs, extractors)
end;

destructor TSortFieldComposite<T>.Destroy;
begin
  FComparators := nil;
  FExtractors := nil;
  FExprs := nil;
  inherited;
end;

initialization

SortString := TSortFieldString.Create();
SortInt64 := TSortFieldInteger.Create();
SortDouble := TSortFieldDouble.Create();
SortBoolean := TSortFieldBoolean.Create();

finalization

SortString := nil;
SortInt64 := nil;
SortDouble := nil;
SortBoolean := nil;

end.
