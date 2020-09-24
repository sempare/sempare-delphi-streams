unit Sempare.Streams.Types;

interface

uses
  System.SysUtils,
  System.Rtti;

type
  EStream = class(Exception);
  EStreamReflect = class(EStream);

  TSortOrder = (soAscending, soDescending, ASC = soAscending, DESC = soDescending);

  TFilterFunction<TInput> = reference to function(const AInput: TInput): boolean;

  TMapFunction<TInput, TOutput> = reference to function(const AInput: TInput): TOutput;
  TApplyFunction<TInput> = reference to procedure(var AInput: TInput);
  FValueFilter = reference to function(const AValue: TValue): boolean;

  IFilterFunction = interface
    ['{E1073079-7967-4723-B6D4-6A9CB533DF30}']
    function IsTrue(const AValue: TValue): boolean;
  end;

  IFilterFunction<T> = interface(IFilterFunction)
    ['{0A3EE696-9714-4389-8EEB-CEF6B7748DD5}']
    function IsTrue(const AValue: T): boolean;
  end;

  IFieldExtractor = interface
    ['{E62E9EBF-7686-40E2-8747-9255208923AD}']
    function GetValue(const AValue: TValue; var Value: TValue): boolean;
    function GetRttiFields: TArray<TRttiField>;
    function GetRttiType: TRttiType;
    property RttiFields: TArray<TRttiField> read GetRttiFields;
    property RttiType: TRttiType read GetRttiType;
  end;

implementation

end.
