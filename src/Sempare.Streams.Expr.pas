unit Sempare.Streams.Expr;

interface

uses
  System.Rtti,
  System.SysUtils,
  Sempare.Streams.Types,
  Sempare.Streams.Rtti;

type
  TUnaryExpr = class;
  TBinaryExpr = class;
  TFieldExpr = class;
  TBoolExpr = class;

  TExprException = class(Exception);

  TExprVisitor = class
    procedure Visit(const AExpr: TBoolExpr); overload; virtual;
    procedure Visit(const AExpr: TUnaryExpr); overload; virtual;
    procedure Visit(const AExpr: TBinaryExpr); overload; virtual;
    procedure Visit(const AExpr: TFieldExpr); overload; virtual;
  end;

  TRttiExprVisitor = class(TExprVisitor)
  strict private
    FType: TRttiType;
  public
    constructor Create(const AType: TRttiType);
    procedure Visit(const AExpr: TFieldExpr); overload; override;
  end;

  TExprType = (etUnary, etBinary, etField, etBoolean, etFilter);

  TExpr = class abstract
  strict protected
    function GetExprType: TExprType; virtual; abstract;
  public
    procedure Accept(const AVisitor: TExprVisitor); virtual;
    function IsTrue(const [ref] AValue: TValue): boolean; virtual; abstract;

    function AsBoolExpr: TBoolExpr;
    function AsUnaryExpr: TUnaryExpr;
    function AsBinaryExpr: TBinaryExpr;
    function AsFieldExpr: TFieldExpr;
    function IsExprType(const AExprType: TExprType): boolean;

    property ExprType: TExprType read GetExprType;
  end;

  TBoolExpr = class(TExpr)
  strict private
    FValue: boolean;
  strict protected
    function GetExprType: TExprType; override;
  public
    constructor Create(const AValue: boolean);
    function IsTrue(const [ref] AValue: TValue): boolean; override;
  end;

  TUnaryExpr = class(TExpr)
  type
    TOper = (uoNOT);
  strict private
    FExpr: TExpr;
    FOP: TOper;
  strict protected
    function GetExprType: TExprType; override;
  public
    constructor Create(const AExpr: TExpr; const AOP: TOper);
    procedure Accept(const AVisitor: TExprVisitor); override;
    function IsTrue(const [ref] AValue: TValue): boolean; override;
  end;

  TBinaryExpr = class(TExpr)
  type
    TOper = (boAND, boOR);
  strict private
    FLeft: TExpr;
    FOP: TOper;
    FRight: TExpr;
  strict protected
    function GetExprType: TExprType; override;
  public
    constructor Create(const ALeft: TExpr; const AOP: TOper; const ARight: TExpr);
    procedure Accept(const AVisitor: TExprVisitor); override;
    function IsTrue(const [ref] AValue: TValue): boolean; override;
  end;

  TFieldExpr = class(TExpr)
  type
    TOper = (foEQ, foLT, foLTE, foGT, foGTE, foNEQ);
  strict private
    FField: string;
    FOP: TOper;
    FValue: TValue;
    FRttiField: IFieldExtractor;
  strict protected
    function GetExprType: TExprType; override;
  public
    constructor Create(const AField: string);
    function IsTrue(const [ref] AValue: TValue): boolean; override;
    property Field: string read FField;
    property OP: TOper read FOP write FOP;
    property Value: TValue read FValue write FValue;
    property RttiField: IFieldExtractor read FRttiField write FRttiField;
  end;

  TFilterExpr = class(TExpr)
  strict private
    FExpr: IFilterFunction;
  strict protected
    function GetExprType: TExprType; override;
  public
    constructor Create(Expr: IFilterFunction);
    function IsTrue(const [ref] AValue: TValue): boolean; override;
  end;

implementation

{ TExprVisitor }

procedure TExprVisitor.Visit(const AExpr: TUnaryExpr);
begin
end;

procedure TExprVisitor.Visit(const AExpr: TBinaryExpr);
begin
end;

procedure TExprVisitor.Visit(const AExpr: TFieldExpr);
begin
end;

procedure TExprVisitor.Visit(const AExpr: TBoolExpr);
begin
end;

{ TRttiExprVisitor }

constructor TRttiExprVisitor.Create(const AType: TRttiType);
begin
  FType := AType;
end;

procedure TRttiExprVisitor.Visit(const AExpr: TFieldExpr);
begin
  AExpr.RttiField := Cache.GetExtractor(FType, AExpr.Field);
  if AExpr.Value.TypeInfo = TypeInfo(TFieldExpr) then
    Visit(AExpr.Value.AsType<TFieldExpr>);
end;

{ TFieldExpr }

constructor TFieldExpr.Create(const AField: string);
begin
  FField := AField.Trim();
end;

function TFieldExpr.IsTrue(const [ref] AValue: TValue): boolean;

  function GetValue(out v: TValue): boolean;
  begin
    result := RttiField.GetValue(AValue, v);
  end;

  function GetRightValue(out v: TValue): boolean;
  begin
    result := true;
    v := FValue;
    if FValue.TypeInfo = TypeInfo(TFieldExpr) then
      result := TFieldExpr(FValue.AsObject).RttiField.GetValue(AValue, v);
  end;

  function RaiseOperatorNotSupported: boolean;
  begin
    raise TExprException.Create('operator not supported');
  end;

  function FilterBoolean(const [ref] A, b: TValue): boolean;

    function GetVal(const A: TValue): boolean;
    begin
      result := A.AsBoolean;
    end;

  var
    ab, bb: boolean;

  begin
    ab := GetVal(A);
    bb := GetVal(b);
    case FOP of
      foEQ:
        result := ab = bb;
      foNEQ:
        result := ab <> bb;
      foLT:
        result := ab < bb;
      foLTE:
        result := ab <= bb;
      foGT:
        result := ab > bb;
      foGTE:
        result := ab >= bb;
    else
      result := RaiseOperatorNotSupported;
    end;
  end;

  function AsInt64(const [ref] AValue: TValue): int64;
  begin
    case AValue.Kind of
      tkInteger, tkInt64:
        result := AValue.AsInt64;
      tkFloat:
        result := trunc(AValue.AsExtended);
    else
      result := 0;
    end;
  end;

  function FilterInt(const [ref] A, b: TValue): boolean;

    function GetVal(const A: TValue): int64;
    begin
      result := A.AsInt64;
    end;

  var
    ab, bb: int64;

  begin
    ab := GetVal(A);
    bb := AsInt64(b);
    case FOP of
      foEQ:
        result := ab = bb;
      foNEQ:
        result := ab <> bb;
      foLT:
        result := ab < bb;
      foLTE:
        result := ab <= bb;
      foGT:
        result := ab > bb;
      foGTE:
        result := ab >= bb;
    else
      result := RaiseOperatorNotSupported;
    end;
  end;

  function AsFloat(const [ref] AValue: TValue): double;
  begin
    case AValue.Kind of
      tkInteger, tkInt64:
        result := AValue.AsInt64;
      tkFloat:
        result := AValue.AsExtended;
    else
      result := 0;
    end;
  end;

  function FilterFloat(const [ref] A, b: TValue): boolean;
    function GetVal(const A: TValue): double;
    begin
      result := A.AsExtended;
    end;

  var
    ab, bb: double;

  begin
    ab := GetVal(A);
    bb := AsFloat(b);
    case FOP of
      foEQ:
        result := ab = bb;
      foNEQ:
        result := ab <> bb;
      foLT:
        result := ab < bb;
      foLTE:
        result := ab <= bb;
      foGT:
        result := ab > bb;
      foGTE:
        result := ab >= bb;
    else
      result := RaiseOperatorNotSupported;
    end;
  end;

  function FilterString(const [ref] A, b: TValue): boolean;
    function GetVal(const A: TValue): string;
    begin
      result := A.AsString;
    end;

  var
    ab, bb: string;
  begin
    ab := GetVal(A);
    bb := GetVal(b);
    case FOP of
      foEQ:
        result := ab = bb;
      foNEQ:
        result := ab <> bb;
      foLT:
        result := ab < bb;
      foLTE:
        result := ab <= bb;
      foGT:
        result := ab > bb;
      foGTE:
        result := ab >= bb;
    else
      result := RaiseOperatorNotSupported;
    end;
  end;

var
  r, v: TValue;

begin
  if not GetValue(v) then
    exit(false);
  if not GetRightValue(r) then
    exit(false);
  case v.Kind of
    tkEnumeration:
      if TypeInfo(boolean) = v.TypeInfo then
        result := FilterBoolean(v, r)
      else
        raise TExprException.Create('enum not supported');
    tkInteger, tkInt64:
      result := FilterInt(v, r);
    tkFloat:
      result := FilterFloat(v, r);
    tkString, tkWideString, tkUnicodeString, tkAnsiString:
      result := FilterString(v, r);
  else
    raise TExprException.Create('field type not supported');
  end;
end;

function TFieldExpr.GetExprType: TExprType;
begin
  result := etField;
end;

{ TBinOp }

procedure TBinaryExpr.Accept(const AVisitor: TExprVisitor);
begin
  FLeft.Accept(AVisitor);
  FRight.Accept(AVisitor);
end;

constructor TBinaryExpr.Create(const ALeft: TExpr; const AOP: TOper; const ARight: TExpr);
begin
  FLeft := ALeft;
  FOP := AOP;
  FRight := ARight;
end;

function TBinaryExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
  result := FLeft.IsTrue(AValue);
  case FOP of
    boAND:
      result := result and FRight.IsTrue(AValue);
    boOR:
      result := result or FRight.IsTrue(AValue);
  end;
end;

function TBinaryExpr.GetExprType: TExprType;
begin
  result := etBinary;
end;

{ TUnaryOp }

procedure TUnaryExpr.Accept(const AVisitor: TExprVisitor);
begin
  FExpr.Accept(AVisitor);
end;

constructor TUnaryExpr.Create(const AExpr: TExpr; const AOP: TOper);
begin
  FExpr := AExpr;
  FOP := AOP;
end;

function TUnaryExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
  case FOP of
    uoNOT:
      result := not FExpr.IsTrue(AValue);
  else
    raise TExprException.Create('unary type not supported');
  end;
end;

function TUnaryExpr.GetExprType: TExprType;
begin
  result := etUnary;
end;

{ TExpr }

procedure TExpr.Accept(const AVisitor: TExprVisitor);
begin
  if self is TFieldExpr then
    AVisitor.Visit(self.AsFieldExpr)
  else if self is TBinaryExpr then
    AVisitor.Visit(self.AsBinaryExpr)
  else if self is TUnaryExpr then
    AVisitor.Visit(self.AsUnaryExpr)
  else if self is TBoolExpr then
    AVisitor.Visit(self.AsBoolExpr)
  else
    raise TExprException.Create('unexpected expression type');
end;

function TExpr.AsBinaryExpr: TBinaryExpr;
begin
  result := self as TBinaryExpr;
end;

function TExpr.AsBoolExpr: TBoolExpr;
begin
  result := self as TBoolExpr;
end;

function TExpr.AsFieldExpr: TFieldExpr;
begin
  result := self as TFieldExpr;
end;

function TExpr.AsUnaryExpr: TUnaryExpr;
begin
  result := self as TUnaryExpr;
end;

function TExpr.IsExprType(const AExprType: TExprType): boolean;
begin
  result := GetExprType = AExprType;
end;

{ TBoolExpr }

constructor TBoolExpr.Create(const AValue: boolean);
begin
  FValue := AValue;
end;

function TBoolExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
  result := FValue;
end;

function TBoolExpr.GetExprType: TExprType;
begin
  result := etBoolean;
end;

{ TFilterExpr }

constructor TFilterExpr.Create(Expr: IFilterFunction);
begin
  FExpr := Expr;
end;

function TFilterExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
  result := FExpr.IsTrue(AValue);
end;

function TFilterExpr.GetExprType: TExprType;
begin
  result := etFilter;
end;

end.
