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

    IVisitableExpr = interface(IExpr)
        ['{C3242CA6-1996-41AB-BC8A-1281183DA76F}']
        procedure Accept(const AVisitor: TExprVisitor);
    end;

    TRttiExprVisitor = class(TExprVisitor)
    strict private
        FType: TRttiType;
    public
        constructor Create(const AType: TRttiType);
        procedure Visit(const AExpr: TFieldExpr); overload; override;
    end;

    TExpr = class abstract(TInterfacedObject, IExpr, IVisitableExpr)
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
        FExpr: IExpr;
        FOP: TOper;
    strict protected
        function GetExprType: TExprType; override;
    public
        constructor Create(AExpr: IExpr; const AOP: TOper);
        destructor Destroy; override;

        procedure Accept(const AVisitor: TExprVisitor); override;
        function IsTrue(const [ref] AValue: TValue): boolean; override;
    end;

    TBinaryExpr = class(TExpr)
    type
        TOper = (boAND, boOR);
    strict private
        FLeft: IExpr;
        FOP: TOper;
        FRight: IExpr;
    strict protected
        function GetExprType: TExprType; override;
    public
        constructor Create(ALeft: IExpr; const AOP: TOper; ARight: IExpr);
        destructor Destroy; override;
        procedure Accept(const AVisitor: TExprVisitor); override;
        function IsTrue(const [ref] AValue: TValue): boolean; override;
    end;

    TFieldExpr = class(TExpr, IFieldExpr)
    strict private
        FField: string;
        FOP: TFieldExprOper;
        FValue: TValue;
        FRttiField: IFieldExtractor;
        function GetField: string;
        function GetOP: TFieldExprOper;
        function GetRttiField: IFieldExtractor;
        function GetValue: TValue;
        procedure SetOP(const Value: TFieldExprOper);
        procedure SetRttiField(const Value: IFieldExtractor);
        procedure SetValue(const Value: TValue);
    strict protected
        function GetExprType: TExprType; override;
    public
        constructor Create(const AField: string);
        destructor Destroy; override;
        function IsTrue(const [ref] AValue: TValue): boolean; override;
        property Field: string read GetField;
        property OP: TFieldExprOper read GetOP write SetOP;
        property Value: TValue read GetValue write SetValue;
        property RttiField: IFieldExtractor read GetRttiField write SetRttiField;
    end;

    TFilterExpr = class(TExpr)
    strict private
        FExpr: IFilterFunction;
    strict protected
        function GetExprType: TExprType; override;
    public
        constructor Create(Expr: IFilterFunction);
        destructor Destroy; override;

        function IsTrue(const [ref] AValue: TValue): boolean; override;
    end;

implementation

uses
    System.TypInfo; // added to get rid of warning

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
    AExpr.RttiField := StreamCache.GetExtractor(FType, AExpr.Field);
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
        exit(RttiField.GetValue(AValue, v));
    end;

    function GetRightValue(out v: TValue): boolean;
    begin
        v := FValue;
        if FValue.TypeInfo = TypeInfo(TFieldExpr) then
            exit(TFieldExpr(FValue.AsObject).RttiField.GetValue(AValue, v))
        else
            exit(true);
    end;

    function RaiseOperatorNotSupported: boolean;
    begin
        raise TExprException.Create('operator not supported');
    end;

    function FilterBoolean(const [ref] A, b: TValue): boolean;

        function GetVal(const A: TValue): boolean;
        begin
            exit(A.AsBoolean);
        end;

    var
        ab, bb: boolean;

    begin
        ab := GetVal(A);
        bb := GetVal(b);
        case FOP of
            foEQ:
                exit(ab = bb);
            foNEQ:
                exit(ab <> bb);
            foLT:
                exit(ab < bb);
            foLTE:
                exit(ab <= bb);
            foGT:
                exit(ab > bb);
            foGTE:
                exit(ab >= bb);
        else
            exit(RaiseOperatorNotSupported);
        end;
    end;

    function AsInt64(const [ref] AValue: TValue): int64;
    begin
        case AValue.Kind of
            tkInteger, tkInt64:
                exit(AValue.AsInt64);
            tkFloat:
                exit(trunc(AValue.AsExtended));
        else
            exit(0);
        end;
    end;

    function FilterInt(const [ref] A, b: TValue): boolean;

        function GetVal(const A: TValue): int64;
        begin
            exit(A.AsInt64);
        end;

    var
        ab, bb: int64;

    begin
        ab := GetVal(A);
        bb := AsInt64(b);
        case FOP of
            foEQ:
                exit(ab = bb);
            foNEQ:
                exit(ab <> bb);
            foLT:
                exit(ab < bb);
            foLTE:
                exit(ab <= bb);
            foGT:
                exit(ab > bb);
            foGTE:
                exit(ab >= bb);
        else
            exit(RaiseOperatorNotSupported);
        end;
    end;

    function AsFloat(const [ref] AValue: TValue): double;
    begin
        case AValue.Kind of
            tkInteger, tkInt64:
                exit(AValue.AsInt64);
            tkFloat:
                exit(AValue.AsExtended);
        else
            exit(0);
        end;
    end;

    function FilterFloat(const [ref] A, b: TValue): boolean;
        function GetVal(const A: TValue): double;
        begin
            exit(A.AsExtended);
        end;

    var
        ab, bb: double;

    begin
        ab := GetVal(A);
        bb := AsFloat(b);
        case FOP of
            foEQ:
                exit(ab = bb);
            foNEQ:
                exit(ab <> bb);
            foLT:
                exit(ab < bb);
            foLTE:
                exit(ab <= bb);
            foGT:
                exit(ab > bb);
            foGTE:
                exit(ab >= bb);
        else
            exit(RaiseOperatorNotSupported);
        end;
    end;

    function FilterString(const [ref] A, b: TValue): boolean;
        function GetVal(const A: TValue): string;
        begin
            exit(A.AsString);
        end;

    var
        ab, bb: string;
    begin
        ab := GetVal(A);
        bb := GetVal(b);
        case FOP of
            foEQ:
                exit(ab = bb);
            foNEQ:
                exit(ab <> bb);
            foLT:
                exit(ab < bb);
            foLTE:
                exit(ab <= bb);
            foGT:
                exit(ab > bb);
            foGTE:
                exit(ab >= bb);
        else
            exit(RaiseOperatorNotSupported);
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
                exit(FilterBoolean(v, r))
            else
                raise TExprException.Create('enum not supported');
        tkInteger, tkInt64:
            exit(FilterInt(v, r));
        tkFloat:
            exit(FilterFloat(v, r));
        tkString, tkWideString, tkUnicodeString, tkAnsiString:
            exit(FilterString(v, r));
    else
        raise TExprException.Create('field type not supported');
    end;
end;

procedure TFieldExpr.SetOP(const Value: TFieldExprOper);
begin
    Self.FOP := Value;
end;

procedure TFieldExpr.SetRttiField(const Value: IFieldExtractor);
begin
    FRttiField := Value;
end;

procedure TFieldExpr.SetValue(const Value: TValue);
begin
    FValue := Value;
end;

destructor TFieldExpr.Destroy;
begin
    FRttiField := nil;
    inherited;
end;

function TFieldExpr.GetExprType: TExprType;
begin
    exit(etField);
end;

function TFieldExpr.GetField: string;
begin
    exit(FField);
end;

function TFieldExpr.GetOP: TFieldExprOper;
begin
    exit(FOP);
end;

function TFieldExpr.GetRttiField: IFieldExtractor;
begin
    exit(FRttiField);
end;

function TFieldExpr.GetValue: TValue;
begin
    exit(FValue);
end;

{ TBinOp }

procedure TBinaryExpr.Accept(const AVisitor: TExprVisitor);
var
    e: IVisitableExpr;
begin
    if supports(FLeft, IVisitableExpr, e) then
        e.Accept(AVisitor);
    if supports(FRight, IVisitableExpr, e) then
        e.Accept(AVisitor);
end;

constructor TBinaryExpr.Create(ALeft: IExpr; const AOP: TOper; ARight: IExpr);
begin
    FLeft := ALeft;
    FOP := AOP;
    FRight := ARight;
end;

destructor TBinaryExpr.Destroy;
begin
    FLeft := nil;
    FRight := nil;
    inherited;
end;

function TBinaryExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
    result := FLeft.IsTrue(AValue);
    case FOP of
        boAND:
            exit(result and FRight.IsTrue(AValue));
        boOR:
            exit(result or FRight.IsTrue(AValue));
    end;
end;

function TBinaryExpr.GetExprType: TExprType;
begin
    exit(etBinary);
end;

{ TUnaryOp }

procedure TUnaryExpr.Accept(const AVisitor: TExprVisitor);
var
    e: IVisitableExpr;
begin
    if supports(FExpr, IVisitableExpr, e) then
        e.Accept(AVisitor);
end;

constructor TUnaryExpr.Create(AExpr: IExpr; const AOP: TOper);
begin
    FExpr := AExpr;
    FOP := AOP;
end;

destructor TUnaryExpr.Destroy;
begin
    FExpr := nil;
    inherited;
end;

function TUnaryExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
    case FOP of
        uoNOT:
            exit(not FExpr.IsTrue(AValue));
    else
        raise TExprException.Create('unary type not supported');
    end;
end;

function TUnaryExpr.GetExprType: TExprType;
begin
    exit(etUnary);
end;

{ TExpr }

procedure TExpr.Accept(const AVisitor: TExprVisitor);
begin
    if Self is TFieldExpr then
        AVisitor.Visit(AsFieldExpr)
    else if Self is TBinaryExpr then
        AVisitor.Visit(AsBinaryExpr)
    else if Self is TUnaryExpr then
        AVisitor.Visit(AsUnaryExpr)
    else if Self is TBoolExpr then
        AVisitor.Visit(AsBoolExpr)
    else
        raise TExprException.Create('unexpected expression type');
end;

function TExpr.AsBinaryExpr: TBinaryExpr;
begin
    exit(Self as TBinaryExpr);
end;

function TExpr.AsBoolExpr: TBoolExpr;
begin
    exit(Self as TBoolExpr);
end;

function TExpr.AsFieldExpr: TFieldExpr;
begin
    exit(Self as TFieldExpr);
end;

function TExpr.AsUnaryExpr: TUnaryExpr;
begin
    exit(Self as TUnaryExpr);
end;

function TExpr.IsExprType(const AExprType: TExprType): boolean;
begin
    exit(GetExprType = AExprType);
end;

{ TBoolExpr }

constructor TBoolExpr.Create(const AValue: boolean);
begin
    FValue := AValue;
end;

function TBoolExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
    exit(FValue);
end;

function TBoolExpr.GetExprType: TExprType;
begin
    exit(etBoolean);
end;

{ TFilterExpr }

constructor TFilterExpr.Create(Expr: IFilterFunction);
begin
    FExpr := Expr;
end;

function TFilterExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
    exit(FExpr.IsTrue(AValue));
end;

destructor TFilterExpr.Destroy;
begin
    FExpr := nil;
    inherited;
end;

function TFilterExpr.GetExprType: TExprType;
begin
    exit(etFilter);
end;

end.
