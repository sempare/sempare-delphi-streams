unit Sempare.Streams.Rtti;

interface

uses
  Sempare.Streams.Types,
  System.SyncObjs,
  System.TypInfo,
  System.Generics.Collections,
  System.Rtti;

type
  TCache = class
  strict private
    FLock: TCriticalSection;
    FMethods: TDictionary<ptypeinfo, TRttiInvokableType>;
    FTypes: TDictionary<ptypeinfo, TRttiType>;
    FExtractors: TDictionary<TArray<TRttiField>, IFieldExtractor>;
  public
    constructor Create;
    destructor Destroy; override;
    function GetMethod(const AInfo: ptypeinfo): TRttiInvokableType;
    function GetType(const AInfo: ptypeinfo): TRttiType;
    function GetExtractor(const A: TArray<TRttiField>): IFieldExtractor; overload;
    function GetExtractor(const AType: TRttiType; const A: string): IFieldExtractor; overload;
  end;

function GetFieldsFromString(const AType: TRttiType; const A: string): TArray<TRttiField>;

var
  RttiCtx: TRttiContext;
  Cache: TCache;

implementation

uses
  System.SysUtils;

type
  TFieldExtractor = class(TInterfacedObject, IFieldExtractor)
  private
    FRttiField: TArray<TRttiField>;
    function GetRttiFields: TArray<TRttiField>;
    function GetRttiType: TRttiType;
  public
    constructor Create(const AFields: TArray<TRttiField>);
    function GetValue(const AValue: TValue; var Value: TValue): boolean;
    property RttiFields: TArray<TRttiField> read GetRttiFields;
    property RttiType: TRttiType read GetRttiType;
  end;

  { TCache }

constructor TCache.Create;
begin
  FLock := TCriticalSection.Create;
  FMethods := TDictionary<ptypeinfo, TRttiInvokableType>.Create;
  FTypes := TDictionary<ptypeinfo, TRttiType>.Create;
  FExtractors := TDictionary<TArray<TRttiField>, IFieldExtractor>.Create;
end;

destructor TCache.Destroy;
begin
  FLock.Free;
  FExtractors.Free;
  FMethods.Free;
  FTypes.Free;
  inherited;
end;

function TCache.GetExtractor(const A: TArray<TRttiField>): IFieldExtractor;
begin
  result := nil;
  FLock.Acquire;
  try
    if not FExtractors.TryGetValue(A, result) then
    begin
      result := TFieldExtractor.Create(A);
      FExtractors.Add(A, result);
    end;
  finally
    FLock.Release;
  end;
end;

function GetFieldsFromString(const AType: TRttiType; const A: string): TArray<TRttiField>;
var
  parts: TArray<string>;
  f: TRttiField;
  i: integer;
  numparts: integer;
begin
  parts := A.trim.Split(['.']);
  numparts := length(parts);
  f := AType.GetField(parts[0]);
  setlength(result, 1);
  result[0] := f;
  for i := 1 to numparts - 1 do
  begin
    f := f.FieldType.GetField(parts[i]);
    if f = nil then
      raise Exception.Create('field not found');
    insert(f, result, length(result));
  end;
end;

function TCache.GetExtractor(const AType: TRttiType; const A: string): IFieldExtractor;
begin
  result := GetExtractor(GetFieldsFromString(AType, A));
end;

function TCache.GetMethod(const AInfo: ptypeinfo): TRttiInvokableType;
begin
  result := nil;
  FLock.Acquire;
  try
    if not FMethods.TryGetValue(AInfo, result) then
    begin
      result := RttiCtx.GetType(AInfo) as TRttiInvokableType;
      FMethods.Add(AInfo, result);
    end;
  finally
    FLock.Release;
  end;
end;

function TCache.GetType(const AInfo: ptypeinfo): TRttiType;
begin
  result := nil;
  FLock.Acquire;
  try
    if not FTypes.TryGetValue(AInfo, result) then
    begin
      result := RttiCtx.GetType(AInfo);
      FTypes.Add(AInfo, result);
    end;
  finally
    FLock.Release;
  end;
end;

{ TFieldExtractor }

constructor TFieldExtractor.Create(const AFields: TArray<TRttiField>);
begin
  if length(AFields) = 0 then
    raise Exception.Create('fields expected');
  FRttiField := AFields;

end;

function TFieldExtractor.GetRttiFields: TArray<TRttiField>;
begin
  result := FRttiField;
end;

function TFieldExtractor.GetRttiType: TRttiType;
begin
  result := FRttiField[high(FRttiField)].FieldType;
end;

function TFieldExtractor.GetValue(const AValue: TValue; var Value: TValue): boolean;
var
  f: TRttiField;
  o: tobject;
begin
  result := false;
  Value := AValue;
  for f in FRttiField do
  begin
    case Value.Kind of
      tkRecord:
        begin
          Value := f.GetValue(Value.GetReferenceToRawData);
          exit(true);
        end;
      tkClass:
        begin
          o := Value.AsObject;
          if o = nil then
            exit(false);
          Value := f.GetValue(o);
          exit(true);
        end
    else
      exit(true);
    end;
  end;
end;

initialization

Cache := TCache.Create;

finalization

Cache.Free;

end.
