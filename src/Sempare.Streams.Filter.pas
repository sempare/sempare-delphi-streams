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
unit Sempare.Streams.Filter;

interface

uses
  Sempare.Streams.Types,
  Sempare.Streams.Expr,
  System.Rtti;

type
  TAbstractFilter<T> = class abstract(TInterfacedObject, IFilterFunction, IFilterFunction<T>)
  public
    function IsTrue(const AData: TValue): boolean; overload; virtual; abstract;
    function IsTrue(const AData: T): boolean; overload;
  end;

  TExprFilter<T> = class(TAbstractFilter<T>)
  strict private
    FExpr: IExpr;
  public
    constructor Create(AExpr: IExpr);
    destructor Destroy(); override;
    function IsTrue(const AData: TValue): boolean; overload; override;
  end;

  TTypedFunctionFilter<T> = class(TAbstractFilter<T>)
  strict private
    FFunction: TFilterFunction<T>;
  public
    constructor Create(const AFunction: TFilterFunction<T>);
    function IsTrue(const AData: TValue): boolean; override;
  end;

implementation

uses
  System.SysUtils,
  Sempare.Streams.Rtti;

{ TExprFilter<T> }

constructor TExprFilter<T>.Create(AExpr: IExpr);
var
  visitor: TRttiExprVisitor;
  e: IVisitableExpr;
begin
  visitor := TRttiExprVisitor.Create(RttiCtx.GetType(typeinfo(T)));
  try
    if supports(AExpr, IVisitableExpr, e) then
      e.Accept(visitor);
  finally
    visitor.Free;
  end;
  FExpr := AExpr;
end;

destructor TExprFilter<T>.Destroy;
begin
  FExpr := nil;
  inherited;
end;

function TExprFilter<T>.IsTrue(const AData: TValue): boolean;
begin
  result := FExpr.IsTrue(AData);
end;

{ TTypedFunctionFilter<T> }

constructor TTypedFunctionFilter<T>.Create(const AFunction: TFilterFunction<T>);
begin
  FFunction := AFunction;
end;

function TTypedFunctionFilter<T>.IsTrue(const AData: TValue): boolean;
begin
  result := FFunction(AData.AsType<T>());
end;

{ TFilterProcessor<T> }

function TAbstractFilter<T>.IsTrue(const AData: T): boolean;
begin
  result := IsTrue(TValue.From<T>(AData));
end;

end.
