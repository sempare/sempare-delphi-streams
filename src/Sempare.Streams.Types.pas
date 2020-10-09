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
unit Sempare.Streams.Types;

interface

uses
  System.SysUtils,
  System.Generics.Collections,
  System.Rtti;

type
  EStream = class(Exception);
  EStreamReflect = class(EStream);
  EStreamItemNotFound = class(EStream);

  TSortOrder = (soAscending, soDescending, ASC = soAscending, DESC = soDescending);

  TFilterFunction<TInput> = reference to function(const AInput: TInput): boolean;

  TJoinOnFunction<T, TOther> = reference to function(const A: T; const B: TOther): boolean;
  TJoinSelectFunction<T, TOther, TJoined> = reference to function(const A: T; const B: TOther): TJoined;
  TMapFunction<TInput, TOutput> = reference to function(const AInput: TInput): TOutput;

  // AInput is var rather than const [ref] to simplify what developers have to type.
  // It is a minor optimisation when records are used. Note that
  // you can change values in AInput, but changes to AInput itself will have no result.
  TApplyFunction<TInput> = reference to procedure(var AInput: TInput);
  FValueFilter = reference to function(const AValue: TValue): boolean;

  TExprType = (etUnary, etBinary, etField, etBoolean, etFilter);

  IExpr = interface
    ['{90205885-54DA-4E53-A635-CC172BC19D15}']

    function IsTrue(const [ref] AValue: TValue): boolean;
    function GetExprType: TExprType;

    function IsExprType(const AExprType: TExprType): boolean;

    property ExprType: TExprType read GetExprType;

  end;

  IEnum<T> = interface
    ['{B5EAE436-8EE0-404A-B842-E6BD90B23E6F}']
    function EOF: boolean;
    procedure Next;
    function Current: T;
    function HasMore: boolean;
  end;

  IEnumCache<T> = interface
    ['{704AB8CE-4AD0-4166-A235-F0B2F8C0A20D}']
    function GetEnum: IEnum<T>;
    function GetCache: TList<T>;
  end;

  IFieldExtractor = interface
    ['{E62E9EBF-7686-40E2-8747-9255208923AD}']
    function GetValue(const AValue: TValue; var Value: TValue): boolean;
    function GetRttiFields: TArray<TRttiField>;
    function GetRttiType: TRttiType;
    property RttiFields: TArray<TRttiField> read GetRttiFields;
    property RttiType: TRttiType read GetRttiType;
  end;

  TFieldExprOper = (foEQ, foLT, foLTE, foGT, foGTE, foNEQ);

  IFieldExpr = interface(IExpr)
    ['{E99A1E39-384C-4323-8F5A-B08B3B2EEBAD}']

    function GetField: string;
    function GetOP: TFieldExprOper;
    function GetRttiField: IFieldExtractor;
    function GetValue: TValue;
    procedure SetOP(const Value: TFieldExprOper);
    procedure SetRttiField(const Value: IFieldExtractor);
    procedure SetValue(const Value: TValue);

    property Field: string read GetField;
    property OP: TFieldExprOper read GetOP write SetOP;
    property Value: TValue read GetValue write SetValue;
    property RttiField: IFieldExtractor read GetRttiField write SetRttiField;

  end;

  IFilterFunction = interface
    ['{E1073079-7967-4723-B6D4-6A9CB533DF30}']
    function IsTrue(const AValue: TValue): boolean;
  end;

  IFilterFunction<T> = interface(IFilterFunction)
    ['{0A3EE696-9714-4389-8EEB-CEF6B7748DD5}']
    function IsTrue(const AValue: T): boolean;
  end;

implementation

end.
