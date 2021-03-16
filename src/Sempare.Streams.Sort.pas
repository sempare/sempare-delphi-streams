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

  TBaseComparer = class abstract(TInterfacedObject, IComparer<TValue>)
  public
    function Compare(const A, B: TValue): integer; virtual; abstract;
  end;

  TStringComparer = class(TBaseComparer)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TIntegerComparer = class(TBaseComparer)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TDoubleComparer = class(TBaseComparer)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TBooleanComparer = class(TBaseComparer)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TClassOrRecordComparer<T> = class abstract(TInterfacedObject, IComparer<T>)
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

  TReverseComparer<T> = class(TComparer<T>)
  private
    FComparer: IComparer<T>;
  public
    constructor Create(Comparer: IComparer<T>);
    destructor Destroy; override;
    function Compare(const Left, Right: T): integer; override;
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
  exit(FField);
end;

function TSortExpr.GetName: string;
begin
  exit(FName);
end;

function TSortExpr.GetOrder: TSortOrder;
begin
  exit(FOrder);
end;

procedure TSortExpr.SetField(const Value: TRttiField);
begin
  FField := Value;
end;

{ TStringComparer }

function TStringComparer.Compare(const A, B: TValue): integer;
var
  avs, bvs: string;
begin
  avs := A.asstring;
  bvs := B.asstring;
  if avs < bvs then
    exit(-1)
  else if avs = bvs then
    exit(0)
  else
    exit(1);
end;

{ TIntegerComparer }

function TIntegerComparer.Compare(const A, B: TValue): integer;
var
  avs, bvs: int64;
begin
  avs := A.AsInt64;
  bvs := B.AsInt64;
  if avs < bvs then
    exit(-1)
  else if avs = bvs then
    exit(0)
  else
    exit(1);
end;

{ TDoubleComparer }

function TDoubleComparer.Compare(const A, B: TValue): integer;
var
  avs, bvs: double;
begin
  avs := A.AsExtended;
  bvs := B.AsExtended;
  if avs < bvs then
    exit(-1)
  else if avs = bvs then
    exit(0)
  else
    exit(1);
end;

{ TBooleanComparer }

function TBooleanComparer.Compare(const A, B: TValue): integer;
var
  avs, bvs: boolean;
begin
  avs := A.AsBoolean;
  bvs := B.AsBoolean;
  if avs < bvs then
    exit(-1)
  else if avs = bvs then
    exit(0)
  else
    exit(1);
end;

{ TClassOrRecordComparer<T> }

function TClassOrRecordComparer<T>.Compare(const A, B: T): integer;
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
  exit(0);
end;

constructor TClassOrRecordComparer<T>.Create(AComparators: TArray<IComparer<TValue>>; AExprs: TArray<ISortExpr>; AExtractors: TArray<IFieldExtractor>);
begin
  FComparators := AComparators;
  FExtractors := AExtractors;
  FExprs := AExprs;
end;

constructor TClassOrRecordComparer<T>.Create(AExprs: TArray<ISortExpr>);
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
      raise EStream.Create('type not supported');
    end;
    insert(Comparer, comparators, length(comparators));
  end;
  if length(AExprs) = 0 then
    raise EStream.Create('sort expressions expected');

  Create(comparators, AExprs, extractors)
end;

destructor TClassOrRecordComparer<T>.Destroy;
begin
  FComparators := nil;
  FExtractors := nil;
  FExprs := nil;
  inherited;
end;

{ TReverseComparer<T> }

function TReverseComparer<T>.Compare(const Left, Right: T): integer;
begin
  exit(-FComparer.Compare(Left, Right));
end;

constructor TReverseComparer<T>.Create(Comparer: IComparer<T>);
begin
  FComparer := Comparer;
end;

destructor TReverseComparer<T>.Destroy;
begin
  FComparer := nil;
  inherited;
end;

initialization

SortString := TStringComparer.Create();
SortInt64 := TIntegerComparer.Create();
SortDouble := TDoubleComparer.Create();
SortBoolean := TBooleanComparer.Create();

finalization

SortString := nil;
SortInt64 := nil;
SortDouble := nil;
SortBoolean := nil;

end.
