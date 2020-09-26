unit Sempare.Streams.Rtti;

interface

uses
  Sempare.Streams.Types,
  System.SyncObjs,
  System.TypInfo,
  System.Generics.Collections,
  System.Rtti;

type
  TStreamTypeCache = class
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
  StreamCache: TStreamTypeCache;

type
  TObjectHelper = class helper for TObject
  public
    class function SupportsInterface<TC: class; T: IInterface>(const AClass: TC): Boolean; overload; static;
    class function SupportsInterface<T: IInterface>(out Intf: T): Boolean; overload; static;
  end;

function GetInterfaceTypeInfo(InterfaceTable: PInterfaceTable): ptypeinfo;

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
    function GetValue(const AValue: TValue; var Value: TValue): Boolean;
    property RttiFields: TArray<TRttiField> read GetRttiFields;
    property RttiType: TRttiType read GetRttiType;
  end;

function GetInterfaceTypeInfo(InterfaceTable: PInterfaceTable): ptypeinfo;
var
  P: PPointer;
begin
  if Assigned(InterfaceTable) and (InterfaceTable^.EntryCount > 0) then
  begin
    P := Pointer(NativeUInt(@InterfaceTable^.Entries[InterfaceTable^.EntryCount]));
    exit(Pointer(NativeUInt(P^) + SizeOf(Pointer)));
  end
  else
    exit(nil);
end;

{ TStreamTypeCache }

constructor TStreamTypeCache.Create;
begin
  FLock := TCriticalSection.Create;
  FMethods := TDictionary<ptypeinfo, TRttiInvokableType>.Create;
  FTypes := TDictionary<ptypeinfo, TRttiType>.Create;
  FExtractors := TDictionary<TArray<TRttiField>, IFieldExtractor>.Create;
end;

destructor TStreamTypeCache.Destroy;
begin
  FLock.Free;
  FExtractors.Clear;
  FExtractors.Free;
  FMethods.Free;
  FTypes.Free;
  inherited;
end;

function TStreamTypeCache.GetExtractor(const A: TArray<TRttiField>): IFieldExtractor;
begin
  Result := nil;
  FLock.Acquire;
  try
    if not FExtractors.TryGetValue(A, Result) then
    begin
      Result := TFieldExtractor.Create(A);
      FExtractors.Add(A, Result);
    end;
  finally
    FLock.Release;
  end;
end;

function GetFieldsFromString(const AType: TRttiType; const A: string): TArray<TRttiField>;
var
  parts: TArray<string>;
  f: TRttiField;
  I: Integer;
  numparts: Integer;
begin
  parts := A.trim.Split(['.']);
  numparts := length(parts);
  f := AType.GetField(parts[0]);
  setlength(Result, 1);
  Result[0] := f;
  for I := 1 to numparts - 1 do
  begin
    f := f.FieldType.GetField(parts[I]);
    if f = nil then
      raise Exception.Create('field not found');
    insert(f, Result, length(Result));
  end;
end;

function TStreamTypeCache.GetExtractor(const AType: TRttiType; const A: string): IFieldExtractor;
begin
  Result := GetExtractor(GetFieldsFromString(AType, A));
end;

function TStreamTypeCache.GetMethod(const AInfo: ptypeinfo): TRttiInvokableType;
begin
  Result := nil;
  FLock.Acquire;
  try
    if not FMethods.TryGetValue(AInfo, Result) then
    begin
      Result := RttiCtx.GetType(AInfo) as TRttiInvokableType;
      FMethods.Add(AInfo, Result);
    end;
  finally
    FLock.Release;
  end;
end;

function TStreamTypeCache.GetType(const AInfo: ptypeinfo): TRttiType;
begin
  Result := nil;
  FLock.Acquire;
  try
    if not FTypes.TryGetValue(AInfo, Result) then
    begin
      Result := RttiCtx.GetType(AInfo);
      FTypes.Add(AInfo, Result);
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
  Result := FRttiField;
end;

function TFieldExtractor.GetRttiType: TRttiType;
begin
  Result := FRttiField[high(FRttiField)].FieldType;
end;

function TFieldExtractor.GetValue(const AValue: TValue; var Value: TValue): Boolean;
var
  f: TRttiField;
  o: TObject;
begin
  Result := False;
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
            exit(False);
          Value := f.GetValue(o);
          exit(true);
        end
    else
      exit(true);
    end;
  end;
end;

class function TObjectHelper.SupportsInterface<T>(out Intf: T): Boolean;
var
  intfTable: PInterfaceTable;
  IntfTypeInfo: ptypeinfo;
  I: Integer;
begin
  Result := False;
  intfTable := GetInterfaceTable;
  IntfTypeInfo := GetInterfaceTypeInfo(intfTable);
  for I := 0 to intfTable^.EntryCount - 1 do
  begin
    if IntfTypeInfo = TypeInfo(T) then
      exit(true);
    inc(IntfTypeInfo);
  end;
  exit(False);
end;

class function TObjectHelper.SupportsInterface<TC, T>(const AClass: TC): Boolean;
var
  intfTable: PInterfaceTable;
  IntfTypeInfo: ptypeinfo;
  I: Integer;
begin
  Result := False;
  intfTable := AClass.GetInterfaceTable;
  IntfTypeInfo := GetInterfaceTypeInfo(intfTable);
  for I := 0 to intfTable^.EntryCount - 1 do
  begin
    if IntfTypeInfo = TypeInfo(T) then
      exit(true);
    inc(IntfTypeInfo);
  end;
  exit(False);
end;

initialization

StreamCache := TStreamTypeCache.Create;

finalization

StreamCache.Free;

end.
