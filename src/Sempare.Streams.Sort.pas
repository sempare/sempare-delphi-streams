unit Sempare.Streams.Sort;

interface

uses
  System.Generics.Collections,
  System.Generics.Defaults,
  System.Rtti,
  Sempare.Streams.RttiCache,
  Sempare.Streams.Types;

type
  TSortExpr = class
  strict private
    FName: string;
    FOrder: TSortOrder;
    FField: TRttiField;
  public
    constructor Create(const AName: string; const AOrder: TSortOrder);
    property Name: string read FName;
    property Order: TSortOrder read FOrder;
    property RttiField: TRttiField read FField write FField;
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

  TSortFieldComposite = class abstract(TInterfacedObject, IComparer<TValue>)
  strict protected
    FComparators: TArray<IComparer<TValue>>;
    FExtractors: TArray<IFieldExtractor>;
    FExprs: TArray<TSortExpr>;
  public
    constructor Create(const ARttiType: TRttiType; const AExprs: TArray<TSortExpr>); overload;
    constructor Create(const AComparators: TArray<IComparer<TValue>>; const AExprs: TArray<TSortExpr>; const AExtractors: TArray<IFieldExtractor>); overload;
    destructor Destroy; override;
    function Compare(const A, B: TValue): integer;
  end;

implementation

uses
  System.SysUtils;

var
  SortString: IComparer<TValue>;
  SortInt64: IComparer<TValue>;
  SortDouble: IComparer<TValue>;
  SortBoolean: IComparer<TValue>;

  { TSortExpr }

constructor TSortExpr.Create(const AName: string; const AOrder: TSortOrder);
begin
  FName := AName;
  FOrder := AOrder;
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

{ TSortFieldComposite }

function TSortFieldComposite.Compare(const A, B: TValue): integer;
var
  i, cv: integer;
  av, bv: TValue;
  e: IFieldExtractor;
begin
  for i := 0 to length(FExtractors) - 1 do
  begin
    e := FExtractors[i];
    if not e.getvalue(A, av) then
      exit(1);
    if not e.getvalue(B, bv) then
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

constructor TSortFieldComposite.Create(const AComparators: TArray<IComparer<TValue>>; const AExprs: TArray<TSortExpr>; const AExtractors: TArray<IFieldExtractor>);
begin
  FComparators := AComparators;
  FExtractors := AExtractors;
  FExprs := AExprs;
end;

constructor TSortFieldComposite.Create(const ARttiType: TRttiType; const AExprs: TArray<TSortExpr>);
var
  Comparer: IComparer<TValue>;
  comparators: TArray<IComparer<TValue>>;
  extractors: TArray<IFieldExtractor>;
  e: IFieldExtractor;
  Field: TSortExpr;
  i: integer;
begin
  setlength(comparators, 0);
  setlength(extractors, 0);
  for i := 0 to length(AExprs) - 1 do
  begin
    Field := AExprs[i];
    e := cache.GetExtractor(ARttiType, Field.Name);
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

destructor TSortFieldComposite.Destroy;
begin
  setlength(FComparators, 0);
  setlength(FExtractors, 0);
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
