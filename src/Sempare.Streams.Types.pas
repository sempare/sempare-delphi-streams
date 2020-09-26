unit Sempare.Streams.Types;

interface

uses
  System.SysUtils,
  System.Generics.Collections,
  System.Rtti;

type
  EStream = class(Exception);
  EStreamReflect = class(EStream);

  TSortOrder = (soAscending, soDescending, ASC = soAscending, DESC = soDescending);

  TFilterFunction<TInput> = reference to function(const AInput: TInput): boolean;

  TJoinOnFunction<T, TOther> = reference to function(const A: T; const B: TOther): boolean;
  TJoinSelectFunction<T, TOther, TJoined> = reference to function(const A: T; const B: TOther): TJoined;
  TMapFunction<TInput, TOutput> = reference to function(const AInput: TInput): TOutput;
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
    function GetCache : TList<T>;
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
