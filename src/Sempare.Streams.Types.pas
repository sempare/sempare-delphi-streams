unit Sempare.Streams.Types;

interface

uses
  System.Rtti;

type
  TSortOrder = (soAscending, soDescending, ASC = soAscending, DESC = soDescending);

  TFilterFunction<TInput> = reference to function(const AInput: TInput): boolean;

  TMapFunction<TInput, TOutput> = reference to function(const AInput: TInput): TOutput;
  TApplyFunction<TInput> = reference to procedure(var AInput: TInput);
  FValueFilter<TInput> = reference to function(const AValue: TValue): boolean;

  IFilterProcessor = interface
    ['{FAF8D75F-4639-4995-AE08-80EEEFE52C25}']
    function Filter(const AData: TValue): boolean;
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
