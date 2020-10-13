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
unit Sempare.Streams.Spring4d.Test;

interface

uses
  System.Generics.Collections,
  System.SysUtils,
  Sempare.Streams.Test.Common,
  DUnitX.TestFramework;

type

  [TestFixture]
  TSpring4dStreamTest = class(TStreamsTestBase)
  public
    [Setup]
    procedure Setup; override;

    [Teardown]
    procedure Teardown; override;

        [Test]
    procedure TestEmptyList;


    [Test]
    procedure TestList;

    [Test]
    procedure TestSet;
  end;

implementation

uses
  Spring.Collections,
  Sempare.Streams.Spring4d;

{ TSpring4dStreamTest }

procedure TSpring4dStreamTest.Setup;
begin
  inherited;

end;

procedure TSpring4dStreamTest.Teardown;
begin
  inherited;

end;

procedure TSpring4dStreamTest.TestEmptyList;
var
  l: IList<Integer>;
  lst : TArray<integer>;
begin
  l := TCollections.CreateList<Integer>;
  Assert.IsTrue( //
    Stream.From<Integer>(lst) //
    .Equals(Stream.From<Integer>(l)));
end;

procedure TSpring4dStreamTest.TestList;
var
  l: IList<Integer>;
begin
  l := TCollections.CreateList<Integer>;
  l.AddRange([1, 2, 3, 4, 5]);
  Assert.IsTrue( //
    Stream.From<Integer>([1, 2, 3, 4, 5]) //
    .Equals(Stream.From<Integer>(l)));
end;

procedure TSpring4dStreamTest.TestSet;
var
  l: ISet<Integer>;
begin
  l := TCollections.CreateSet<Integer>;
  l.AddRange([1, 2, 3, 4, 5]);
  Assert.IsTrue( //
    Stream.From<Integer>([3, 4, 2, 1, 5]) //
    .Equals(Stream.From<Integer>(l)));
end;


initialization

TDUnitX.RegisterTestFixture(TSpring4dStreamTest);


end.
